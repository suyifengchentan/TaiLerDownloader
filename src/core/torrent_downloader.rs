#![cfg(feature = "torrent")]

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use librqbit::{AddTorrent, AddTorrentOptions, Session, SessionOptions};

use super::downloader::{DownloadConfig, DownloadTask};
use super::downloader_interface::{BaseDownloader, Downloader};
use super::performance_monitor::PerformanceMonitor;

/// BitTorrent Downloader
/// Supports magnet: links, torrent file URLs, DHT network, PEX (Peer Exchange)
/// Based on librqbit - Rust BitTorrent client library
pub struct TorrentDownloader {
    base: BaseDownloader,
    monitor: Option<Arc<PerformanceMonitor>>,
}

impl TorrentDownloader {
    pub async fn new(config: Arc<RwLock<DownloadConfig>>) -> Self {
        let monitor = super::performance_monitor::get_global_monitor().await;

        TorrentDownloader {
            base: BaseDownloader {
                config: Some(config),
                running: true,
                ..Default::default()
            },
            monitor,
        }
    }
}

#[async_trait::async_trait]
impl Downloader for TorrentDownloader {
    async fn download(
        &mut self,
        task: &DownloadTask,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Determine output directory (get parent from save_path)
        let save_path = PathBuf::from(&task.save_path);
        let output_dir = save_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();

        eprintln!("BT download: {} -> {:?}", task.url, output_dir);

        let trackers = if let Some(config) = &self.base.config {
            config.read().await.torrent_trackers.clone()
        } else {
            Vec::new()
        };
        let tracker_count = trackers.len();

        let mut session_opts = SessionOptions {
            disable_dht_persistence: true,
            listen_port_range: Some(6881..6891),
            ..Default::default()
        };

        if !trackers.is_empty() {
            eprintln!(
                "BT using {} configured tracker(s); DHT persistence disabled",
                tracker_count
            );
        }

        // Create librqbit Session. Persistent DHT can fail on some Windows setups,
        // so use a non-persistent DHT first and fall back to tracker-only mode.
        let session = match Session::new_with_opts(output_dir.clone(), session_opts).await {
            Ok(session) => session,
            Err(err) => {
                eprintln!(
                    "BT session init with DHT failed: {:#}. Falling back to tracker-only mode.",
                    err
                );
                session_opts = SessionOptions {
                    disable_dht: true,
                    disable_dht_persistence: true,
                    listen_port_range: Some(6881..6891),
                    ..Default::default()
                };
                Session::new_with_opts(output_dir, session_opts)
                    .await
                    .map_err(|fallback_err| {
                        format!(
                            "Failed to create BT Session: primary init error: {:#}; fallback error: {:#}",
                            err, fallback_err
                        )
                    })?
            }
        };

        // Build AddTorrent parameters. This accepts magnet/http(s) URLs and local
        // .torrent files via the same path.
        let add_torrent = AddTorrent::from_cli_argument(&task.url)
            .map_err(|e| format!("Unsupported BT input {}: {}", task.url, e))?;

        // Add torrent and start download
        let opts = AddTorrentOptions {
            overwrite: true,
            trackers: if trackers.is_empty() {
                None
            } else {
                Some(trackers)
            },
            ..Default::default()
        };

        let response = session
            .add_torrent(add_torrent, Some(opts))
            .await
            .map_err(|e| format!("Failed to add torrent: {}", e))?;

        let handle = match response {
            librqbit::AddTorrentResponse::Added(_, handle) => handle,
            librqbit::AddTorrentResponse::AlreadyManaged(_, handle) => {
                eprintln!("BT torrent already exists, continuing download");
                handle
            }
            librqbit::AddTorrentResponse::ListOnly(_info) => {
                eprintln!("BT torrent added in list-only mode");
                return Err("Torrent added in list-only mode, download not started".into());
            }
        };

        if let Ok((name, total_bytes)) = handle.with_metadata(|metadata| {
            (
                metadata
                    .info
                    .name
                    .as_deref()
                    .map(|name| String::from_utf8_lossy(name).into_owned())
                    .unwrap_or_else(|| "<unknown>".to_string()),
                metadata.info.length.unwrap_or(0),
            )
        }) {
            eprintln!(
                "BT metadata ready: {} ({} bytes), {} extra tracker(s)",
                name,
                total_bytes,
                tracker_count
            );
        }

        eprintln!("BT download started, waiting for completion...");

        // Poll for download completion
        let mut last_reported_bytes: u64 = 0;
        let mut tick: u64 = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            tick += 1;

            let stats = handle.stats();
            let downloaded = stats.progress_bytes;
            let total = stats.total_bytes;

            if tick == 1 || tick % 10 == 0 {
                if let Some(live) = &stats.live {
                    let peers = &live.snapshot.peer_stats;
                    eprintln!(
                        "BT status: {} | peers seen={} queued={} connecting={} live={} dead={} | downloaded={}/{}",
                        stats.state,
                        peers.seen,
                        peers.queued,
                        peers.connecting,
                        peers.live,
                        peers.dead,
                        downloaded,
                        total
                    );
                } else {
                    eprintln!(
                        "BT status: {} | downloaded={}/{}",
                        stats.state, downloaded, total
                    );
                }
            }

            // Update progress
            if let Some(ref monitor) = self.monitor {
                let new_bytes = downloaded.saturating_sub(last_reported_bytes);
                if new_bytes > 0 {
                    if last_reported_bytes == 0 {
                        monitor.set_total_bytes(total as i64);
                    }
                    monitor.add_bytes(new_bytes as i64).await;
                    last_reported_bytes = downloaded;
                }
            }

            // Check if completed
            if downloaded >= total && total > 0 {
                eprintln!(
                    "BT download complete: {:.2} MB",
                    total as f64 / 1024.0 / 1024.0
                );
                break;
            }

            // Check if cancelled
            if !self.base.running {
                eprintln!("BT download cancelled by user");
                return Err("BT download cancelled by user".into());
            }
        }

        Ok(())
    }

    fn get_type(&self) -> String {
        "BitTorrent".to_string()
    }

    async fn cancel(&mut self, _downloader: Box<dyn Downloader>) {
        self.base.running = false;
    }

    async fn get_snapshot(&self) -> Option<Box<dyn std::any::Any>> {
        None
    }
}

impl Default for TorrentDownloader {
    fn default() -> Self {
        TorrentDownloader {
            base: BaseDownloader::new(),
            monitor: None,
        }
    }
}
