use super::downloader::{DownloadChunk, DownloadConfig, DownloadTask};
#[cfg(feature = "socket")]
use super::socket_client::SocketClient;
#[cfg(feature = "websocket")]
use super::websocket_client::WebSocketClient;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

#[async_trait::async_trait]
pub trait Downloader: Send + Sync {
    async fn download(
        &mut self,
        task: &DownloadTask,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn get_type(&self) -> String;
    async fn cancel(&mut self, downloader: Box<dyn Downloader>);
    async fn get_snapshot(&self) -> Option<Box<dyn std::any::Any>>;
}

pub struct BaseDownloader {
    pub total_size: i64,
    pub downloaded: i64,
    pub last_downloaded: i64,
    pub start_time: Instant,
    pub chunks: Vec<DownloadChunk>,
    #[cfg(feature = "websocket")]
    pub ws_client: Option<Arc<tokio::sync::Mutex<WebSocketClient>>>,
    #[cfg(not(feature = "websocket"))]
    pub ws_client: Option<Arc<tokio::sync::Mutex<()>>>,
    #[cfg(feature = "socket")]
    pub socket_client: Option<Arc<tokio::sync::Mutex<SocketClient>>>,
    #[cfg(not(feature = "socket"))]
    pub socket_client: Option<Arc<tokio::sync::Mutex<()>>>,
    pub config: Option<Arc<RwLock<DownloadConfig>>>,
    pub running: bool,
}

impl BaseDownloader {
    pub fn new() -> Self {
        BaseDownloader {
            total_size: 0,
            downloaded: 0,
            last_downloaded: 0,
            start_time: Instant::now(),
            chunks: Vec::new(),
            ws_client: None,
            socket_client: None,
            config: None,
            running: true,
        }
    }

    pub async fn cancel_base(&mut self, _downloader: Box<dyn Downloader>) {
        self.running = false;
    }

    pub async fn get_snapshot_base(&self) -> Option<Box<dyn std::any::Any>> {
        None
    }
}
