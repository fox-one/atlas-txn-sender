use crate::solana_rpc::SolanaRpc;
use dashmap::DashMap;
use futures::StreamExt;
use rand::distributions::Alphanumeric;
use rand::Rng;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_api::config::{RpcBlockSubscribeConfig, RpcBlockSubscribeFilter};
use solana_sdk::clock::UnixTimestamp;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;
use tonic::async_trait;
use tracing::error;

pub struct GrpcGeyserImpl {
    endpoint: String,
    cur_slot: Arc<AtomicU64>,
    signature_cache: Arc<DashMap<String, (UnixTimestamp, Instant)>>,
}

impl GrpcGeyserImpl {
    pub fn new(endpoint: String) -> Self {
        let grpc_geyser = Self {
            endpoint,
            cur_slot: Arc::new(AtomicU64::new(0)),
            signature_cache: Arc::new(DashMap::new()),
        };
        // polling with processed commitment to get latest leaders
        grpc_geyser.poll_slots();
        // polling with confirmed commitment to get confirmed transactions
        // grpc_geyser.poll_blocks();
        grpc_geyser.clean_signature_cache();
        grpc_geyser
    }

    fn clean_signature_cache(&self) {
        let signature_cache = self.signature_cache.clone();
        tokio::spawn(async move {
            loop {
                let signature_cache = signature_cache.clone();
                signature_cache.retain(|_, (_, v)| v.elapsed().as_secs() < 90);
                sleep(Duration::from_secs(60)).await;
            }
        });
    }

    fn poll_blocks(&self) {
        let endpoint = self.endpoint.clone();
        let signature_cache = self.signature_cache.clone();
        tokio::spawn(async move {
            loop {
                let client = PubsubClient::new(&endpoint).await;
                if let Err(e) = client {
                    error!("Error creating pubsub client: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let client = client.unwrap();
                let sub = client
                    .block_subscribe(
                        RpcBlockSubscribeFilter::All,
                        Some(RpcBlockSubscribeConfig {
                            max_supported_transaction_version: Some(0),
                            ..Default::default()
                        }),
                    )
                    .await;
                if let Err(e) = sub {
                    error!("Error subscribing to block: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let (mut accounts, unsubscriber) = sub.unwrap();

                while let Some(resp) = accounts.next().await {
                    let update = resp.value;

                    if let Some(e) = update.err {
                        error!("error in block subscribe: {:?}", e);
                        break;
                    }

                    if let Some(block) = update.block {
                        let block_time = block.block_time.unwrap();
                        for tx in block.transactions.unwrap_or_default() {
                            if let Some(tx) = tx.transaction.decode() {
                                for signature in tx.signatures {
                                    signature_cache.insert(
                                        signature.to_string(),
                                        (block_time, Instant::now()),
                                    );
                                }
                            }
                        }
                    }
                }

                unsubscriber().await;
                sleep(Duration::from_secs(1)).await;
            }
        });
    }

    fn poll_slots(&self) {
        let endpoint = self.endpoint.clone();
        let cur_slot = self.cur_slot.clone();
        tokio::spawn(async move {
            loop {
                let client = PubsubClient::new(&endpoint).await;
                if let Err(e) = client {
                    error!("Error creating pubsub client: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let client = client.unwrap();
                let sub = client.slot_subscribe().await;
                if let Err(e) = sub {
                    error!("Error subscribing to slot: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let (mut accounts, unsubscriber) = sub.unwrap();

                while let Some(slot) = accounts.next().await {
                    cur_slot.store(slot.slot, Ordering::Relaxed);
                }

                unsubscriber().await;
                sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

#[async_trait]
impl SolanaRpc for GrpcGeyserImpl {
    async fn confirm_transaction(&self, signature: String) -> Option<UnixTimestamp> {
        // let start = Instant::now();
        // in practice if a tx doesn't land in less than 60 seconds it's probably not going to land
        // while start.elapsed() < Duration::from_secs(60) {
        //     if let Some(block_time) = self.signature_cache.get(&signature) {
        //         return Some(block_time.0.clone());
        //     }
        //     sleep(Duration::from_millis(10)).await;
        // }
        return None;
    }

    fn get_next_slot(&self) -> Option<u64> {
        let cur_slot = self.cur_slot.load(Ordering::Relaxed);
        if cur_slot == 0 {
            return None;
        }
        Some(cur_slot)
    }
}

fn generate_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}
