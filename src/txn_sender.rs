use solana_client::{
    connection_cache::ConnectionCache, nonblocking::tpu_connection::TpuConnection,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    runtime::{Builder, Runtime},
    time::{sleep, timeout},
};
use tonic::async_trait;
use tracing::{error, warn};

use crate::{
    leader_tracker::LeaderTracker,
    transaction_store::{get_signature, TransactionData, TransactionStore},
};

const RETRY_COUNT_BINS: [i32; 6] = [0, 1, 2, 5, 10, 25];
const MAX_RETRIES_BINS: [i32; 5] = [0, 1, 5, 10, 30];
const MAX_TIMEOUT_SEND_DATA: Duration = Duration::from_millis(500);
const MAX_TIMEOUT_SEND_DATA_BATCH: Duration = Duration::from_millis(500);
const SEND_TXN_RETRIES: usize = 10;

#[async_trait]
pub trait TxnSender: Send + Sync {
    fn send_transaction(&self, txn: TransactionData);
}

pub struct TxnSenderImpl {
    leader_tracker: Arc<dyn LeaderTracker>,
    transaction_store: Arc<dyn TransactionStore>,
    connection_cache: Arc<ConnectionCache>,
    txn_sender_runtime: Arc<Runtime>,
    txn_send_retry_interval_seconds: usize,
    max_retry_queue_size: Option<usize>,
}

impl TxnSenderImpl {
    pub fn new(
        leader_tracker: Arc<dyn LeaderTracker>,
        transaction_store: Arc<dyn TransactionStore>,
        connection_cache: Arc<ConnectionCache>,
        txn_sender_threads: usize,
        txn_send_retry_interval_seconds: usize,
        max_retry_queue_size: Option<usize>,
    ) -> Self {
        let txn_sender_runtime = Builder::new_multi_thread()
            .worker_threads(txn_sender_threads)
            .enable_all()
            .build()
            .unwrap();
        let txn_sender = Self {
            leader_tracker,
            transaction_store,
            connection_cache,
            txn_sender_runtime: Arc::new(txn_sender_runtime),
            txn_send_retry_interval_seconds,
            max_retry_queue_size,
        };
        txn_sender.retry_transactions();
        txn_sender
    }

    fn retry_transactions(&self) {
        let leader_tracker = self.leader_tracker.clone();
        let transaction_store = self.transaction_store.clone();
        let connection_cache = self.connection_cache.clone();
        let txn_sender_runtime = self.txn_sender_runtime.clone();
        let txn_send_retry_interval_seconds = self.txn_send_retry_interval_seconds.clone();
        let max_retry_queue_size = self.max_retry_queue_size.clone();
        tokio::spawn(async move {
            loop {
                let mut transactions_reached_max_retries = vec![];
                let transaction_map = transaction_store.get_transactions();
                let queue_length = transaction_map.len();

                // Shed transactions by retry_count, if necessary.
                if let Some(max_size) = max_retry_queue_size {
                    if queue_length > max_size {
                        warn!(
                            "Transaction retry queue length is over the limit of {}: {}. Load shedding transactions with highest retry count.", 
                            max_size,
                            queue_length
                        );
                        let mut transactions: Vec<(String, TransactionData)> = transaction_map
                            .iter()
                            .map(|x| (x.key().to_owned(), x.value().to_owned()))
                            .collect();
                        transactions.sort_by(|(_, a), (_, b)| a.retry_count.cmp(&b.retry_count));
                        let transactions_to_remove = transactions[(max_size + 1)..].to_vec();
                        for (signature, _) in transactions_to_remove {
                            transaction_store.remove_transaction(signature.clone());
                            transaction_map.remove(&signature);
                        }
                        // let records_dropped = queue_length - max_size;
                        // statsd_gauge!("transactions_retry_queue_dropped", records_dropped as u64);
                    }
                }

                let mut wire_transactions = vec![];
                for mut transaction_data in transaction_map.iter_mut() {
                    wire_transactions.push(transaction_data.wire_transaction.clone());
                    if transaction_data.retry_count >= transaction_data.max_retries {
                        transactions_reached_max_retries
                            .push(get_signature(&transaction_data).unwrap());
                    } else {
                        transaction_data.retry_count += 1;
                    }
                }
                for wire_transaction in wire_transactions.iter() {
                    for leader in leader_tracker.get_leaders() {
                        if leader.tpu_quic.is_none() {
                            error!("leader {:?} has no tpu_quic", leader);
                            continue;
                        }
                        let connection_cache = connection_cache.clone();
                        let leader = Arc::new(leader.clone());
                        let wire_transaction = wire_transaction.clone();
                        txn_sender_runtime.spawn(async move {
                            // retry unless its a timeout
                            for i in 0..SEND_TXN_RETRIES {
                                let conn = connection_cache
                                    .get_nonblocking_connection(&leader.tpu_quic.unwrap());
                                if let Ok(result) = timeout(
                                    MAX_TIMEOUT_SEND_DATA_BATCH,
                                    conn.send_data(&wire_transaction),
                                )
                                .await
                                {
                                    if let Err(e) = result {
                                        if i == SEND_TXN_RETRIES - 1 {
                                            error!(
                                                retry = "true",
                                                "Failed to send transaction batch to {:?}: {}",
                                                leader,
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
                // remove transactions that reached max retries
                for signature in transactions_reached_max_retries {
                    let _ = transaction_store.remove_transaction(signature);
                }
                sleep(Duration::from_secs(txn_send_retry_interval_seconds as u64)).await;
            }
        });
    }
}

#[async_trait]
impl TxnSender for TxnSenderImpl {
    fn send_transaction(&self, transaction_data: TransactionData) {
        for leader in self.leader_tracker.get_leaders() {
            if leader.tpu_quic.is_none() {
                error!("leader {:?} has no tpu_quic", leader);
                continue;
            }
            let connection_cache = self.connection_cache.clone();
            let wire_transaction = transaction_data.wire_transaction.clone();
            self.txn_sender_runtime.spawn(async move {
                for i in 0..SEND_TXN_RETRIES {
                    let conn =
                        connection_cache.get_nonblocking_connection(&leader.tpu_quic.unwrap());
                    if let Ok(result) =
                        timeout(MAX_TIMEOUT_SEND_DATA, conn.send_data(&wire_transaction)).await
                    {
                        if let Err(e) = result {
                            if i == SEND_TXN_RETRIES - 1 {
                                error!(
                                    retry = "false",
                                    "Failed to send transaction to {:?}: {}", leader, e
                                );
                            }
                        }
                    }
                }
            });
        }
    }
}
