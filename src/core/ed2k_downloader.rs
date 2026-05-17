#![cfg(feature = "ed2k")]

use std::sync::Arc;
use tokio::sync::RwLock;

use super::downloader::{DownloadConfig, DownloadTask, UA, default_ed2k_gateways};
use super::downloader_interface::{BaseDownloader, Downloader};
use super::file_utils::create_download_file;
use super::performance_monitor::PerformanceMonitor;

/// ED2K downloader implementation
/// Parses ed2k://|file|<name>|<size>|<hash>|/ format URLs
/// Converts ED2K to HTTP via public gateway: https://ed2k.lyoko.io/hash/<hash>
pub struct ED2KDownloader {
    base: BaseDownloader,
    monitor: Option<Arc<PerformanceMonitor>>,
}

/// Parsed ED2K link info
struct Ed2kInfo {
    name: String,
    size: u64,
    hash: String,
    sources: Vec<String>,
}

impl ED2KDownloader {
    pub async fn new(config: Arc<RwLock<DownloadConfig>>) -> Self {
        let monitor = super::performance_monitor::get_global_monitor().await;
        ED2KDownloader {
            base: BaseDownloader {
                config: Some(config),
                running: true,
                ..Default::default()
            },
            monitor,
        }
    }

    /// Parse ed2k:// URL
    /// Format: ed2k://|file|<filename>|<filesize>|<md4hash>|/
    fn parse_ed2k_url(url: &str) -> Result<Ed2kInfo, String> {
        // Remove ed2k:// prefix
        let stripped = url.strip_prefix("ed2k://").ok_or("Invalid ed2k:// URL")?;

        // Split by |: ["", "file", name, size, hash, "", ""]
        let parts: Vec<&str> = stripped.split('|').collect();

        if parts.len() < 5 {
            return Err(format!(
                "Invalid ED2K URL format, insufficient parts: {}",
                url
            ));
        }

        // Check type (currently only supports 'file')
        if parts[1] != "file" {
            return Err(format!(
                "Unsupported ED2K type: '{}' (only 'file' supported)",
                parts[1]
            ));
        }

        let _name = url::form_urlencoded::parse(parts[2].as_bytes())
            .next()
            .map(|(k, _)| k.into_owned())
            .unwrap_or_else(|| parts[2].to_string());
        // Simple URL decode (filename may have % encoding)
        let name = percent_decode(parts[2]);

        let size: u64 = parts[3]
            .parse()
            .map_err(|_| format!("Failed to parse ED2K file size: '{}'", parts[3]))?;

        let hash = parts[4].to_string();
        if hash.len() != 32 {
            return Err(format!(
                "Invalid ED2K hash length: {} (should be 32)",
                hash.len()
            ));
        }

        let mut sources = Vec::new();
        for part in parts.iter().skip(5) {
            if let Some(source) = part.strip_prefix("s=") {
                let decoded = percent_decode(source);
                if decoded.starts_with("http://") || decoded.starts_with("https://") {
                    sources.push(decoded);
                }
            }
        }

        Ok(Ed2kInfo {
            name,
            size,
            hash,
            sources,
        })
    }
}

fn render_gateway_url(template: &str, ed2k_info: &Ed2kInfo) -> String {
    let template = template.trim();
    if template.is_empty() {
        return String::new();
    }

    if template.contains("{hash}") || template.contains("{name}") || template.contains("{size}") {
        return template
            .replace("{hash}", &ed2k_info.hash)
            .replace("{name}", &ed2k_info.name)
            .replace("{size}", &ed2k_info.size.to_string());
    }

    let trimmed = template.trim_end_matches('/');
    if trimmed.ends_with("/hash") {
        format!("{}/{}", trimmed, ed2k_info.hash)
    } else {
        format!("{}/hash/{}", trimmed, ed2k_info.hash)
    }
}

fn build_candidate_urls(ed2k_info: &Ed2kInfo, gateways: &[String]) -> Vec<String> {
    let mut candidates = Vec::new();

    for source in &ed2k_info.sources {
        if !source.is_empty() && !candidates.iter().any(|candidate| candidate == source) {
            candidates.push(source.clone());
        }
    }

    let gateway_templates = if gateways.is_empty() {
        default_ed2k_gateways()
    } else {
        gateways.to_vec()
    };

    for gateway in gateway_templates {
        let candidate = render_gateway_url(&gateway, ed2k_info);
        if !candidate.is_empty() && !candidates.iter().any(|existing| existing == &candidate) {
            candidates.push(candidate);
        }
    }

    candidates
}

/// Simple URL percent-decode (handles %XX sequences only)
fn percent_decode(s: &str) -> String {
    let mut result = String::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    result.push(byte as char);
                    i += 3;
                    continue;
                }
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

#[async_trait::async_trait]
impl Downloader for ED2KDownloader {
    async fn download(
        &mut self,
        task: &DownloadTask,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ed2k_info = Self::parse_ed2k_url(&task.url)
            .map_err(|e| format!("Failed to parse ED2K URL: {}", e))?;

        eprintln!(
            "ED2K Download: {} ({} bytes, hash={})",
            ed2k_info.name, ed2k_info.size, ed2k_info.hash
        );

        if let Some(ref monitor) = self.monitor {
            monitor.set_total_bytes(ed2k_info.size as i64);
        }

        let configured_gateways = if let Some(config) = &self.base.config {
            config.read().await.ed2k_gateways.clone()
        } else {
            default_ed2k_gateways()
        };

        let candidates = build_candidate_urls(&ed2k_info, &configured_gateways);
        if candidates.is_empty() {
            return Err("No ED2K gateways or direct HTTP sources configured".into());
        }

        // Use reqwest for streaming download
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent(UA)
            .build()
            .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

        let mut failures = Vec::new();

        for candidate in candidates {
            eprintln!("Trying ED2K source: {}", candidate);

            let response = match client.get(&candidate).send().await {
                Ok(response) => response,
                Err(err) => {
                    failures.push(format!("{} => {}", candidate, err));
                    continue;
                }
            };

            let status = response.status();
            if !status.is_success() {
                failures.push(format!(
                    "{} => HTTP {} {}",
                    candidate,
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown")
                ));
                continue;
            }

            let total = response.content_length().unwrap_or(ed2k_info.size) as i64;
            if let Some(ref monitor) = self.monitor {
                monitor.set_total_bytes(total);
            }

            let mut file = create_download_file(&task.save_path, Some(total)).await?;

            let mut stream = response.bytes_stream();
            let mut downloaded: i64 = 0;

            use futures::StreamExt;
            use tokio::io::AsyncWriteExt;

            while let Some(chunk) = stream.next().await {
                let bytes =
                    chunk.map_err(|e| format!("Stream read error from {}: {}", candidate, e))?;
                file.write_all(&bytes)
                    .await
                    .map_err(|e| format!("Write failed for {}: {}", candidate, e))?;
                downloaded += bytes.len() as i64;
                if let Some(ref monitor) = self.monitor {
                    monitor.add_bytes(bytes.len() as i64).await;
                }
            }

            eprintln!(
                "ED2K download complete: {:.2} MB ({})",
                downloaded as f64 / 1024.0 / 1024.0,
                ed2k_info.name
            );
            return Ok(());
        }

        Err(format!(
            "All ED2K sources failed for hash {}\n{}",
            ed2k_info.hash,
            failures
                .into_iter()
                .map(|failure| format!("  {}", failure))
                .collect::<Vec<_>>()
                .join("\n")
        )
        .into())
    }

    fn get_type(&self) -> String {
        "ED2K".to_string()
    }

    async fn cancel(&mut self, _downloader: Box<dyn Downloader>) {
        self.base.running = false;
    }

    async fn get_snapshot(&self) -> Option<Box<dyn std::any::Any>> {
        None
    }
}

impl Default for ED2KDownloader {
    fn default() -> Self {
        ED2KDownloader {
            base: BaseDownloader::new(),
            monitor: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ED2KDownloader, build_candidate_urls, render_gateway_url};

    #[test]
    fn parses_embedded_http_sources() {
        let parsed = ED2KDownloader::parse_ed2k_url(
            "ed2k://|file|example.iso|123|0123456789ABCDEF0123456789ABCDEF|s=https%3A%2F%2Fmirror.example%2Fexample.iso|/",
        )
        .expect("ed2k url should parse");

        assert_eq!(parsed.sources, vec!["https://mirror.example/example.iso"]);
    }

    #[test]
    fn renders_gateway_templates_and_dedupes_sources() {
        let parsed = ED2KDownloader::parse_ed2k_url(
            "ed2k://|file|example.iso|123|0123456789ABCDEF0123456789ABCDEF|s=https://mirror.example/example.iso|/",
        )
        .expect("ed2k url should parse");

        let direct = render_gateway_url("https://gateway.example/hash/{hash}", &parsed);
        assert_eq!(
            direct,
            "https://gateway.example/hash/0123456789ABCDEF0123456789ABCDEF"
        );

        let candidates = build_candidate_urls(
            &parsed,
            &[
                "https://mirror.example/example.iso".to_string(),
                "https://gateway.example".to_string(),
            ],
        );
        assert_eq!(
            candidates,
            vec![
                "https://mirror.example/example.iso".to_string(),
                "https://gateway.example/hash/0123456789ABCDEF0123456789ABCDEF".to_string(),
            ]
        );
    }
}
