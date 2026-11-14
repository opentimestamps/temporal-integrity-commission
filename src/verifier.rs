use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use reqwest::{Client, Url};
use std::collections::VecDeque;
use std::fmt::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::range::EntryRange;
use crate::verify::VerifyResult;
use crate::AppError;

pub struct Stats {
    pub ok: usize,
    pub errors: usize,
    pub missing: usize,
    pub verified: usize,
    pub chain_unverified: usize,
    pub pending: usize,
}

/// Tracks completion times in a rolling 60-second window.
#[derive(Clone)]
struct RateTracker {
    inner: Arc<std::sync::Mutex<VecDeque<std::time::Instant>>>,
}

impl RateTracker {
    fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(VecDeque::new())),
        }
    }

    fn record(&self) {
        let mut deque = self.inner.lock().unwrap();
        deque.push_back(std::time::Instant::now());
    }

    /// Commitments per second averaged over the last 60 seconds.
    fn rate_per_second(&self) -> f64 {
        let mut deque = self.inner.lock().unwrap();
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(60);
        while let Some(&front) = deque.front() {
            if front < cutoff {
                deque.pop_front();
            } else {
                break;
            }
        }
        let count = deque.len();
        if count == 0 {
            return 0.0;
        }
        let oldest = *deque.front().unwrap();
        let elapsed = std::time::Instant::now().duration_since(oldest).as_secs_f64();
        if elapsed == 0.0 {
            return count as f64;
        }
        count as f64 / elapsed
    }
}

/// Verify each 44-byte chunk by concurrently fetching its timestamp proof and
/// verifying it against a local Bitcoin node when available.
pub async fn verify_chunks(
    client: &Client,
    base_url: Url,
    mut stream: impl futures::Stream<Item = Result<Vec<u8>, AppError>> + Unpin + Send,
    pb: &ProgressBar,
    concurrency: usize,
    verbose: bool,
    range: EntryRange,
    btc: Option<std::sync::Arc<bitcoincore_rpc::Client>>,
) -> Result<Stats, AppError> {
    let ok = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let missing = Arc::new(AtomicUsize::new(0));
    let verified = Arc::new(AtomicUsize::new(0));
    let chain_unverified = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(AtomicUsize::new(0));

    let rate_tracker = RateTracker::new();
    let latest_url = Arc::new(std::sync::Mutex::new(String::from("starting...")));

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));

    // Install the two-line template.
    let rt = rate_tracker.clone();
    let url_display = latest_url.clone();
    pb.set_style(
        ProgressStyle::with_template(
            "{url}\n{spinner:.cyan} [{elapsed_precise}] {pos} commitments verified @ {rate:.1}/s ({msg})",
        )
        .unwrap()
        .with_key("rate", move |_: &ProgressState, w: &mut dyn Write| {
            let rate = rt.rate_per_second();
            write!(w, "{:.1}", rate).unwrap();
        })
        .with_key("url", move |_: &ProgressState, w: &mut dyn Write| {
            let url = url_display.lock().unwrap();
            write!(w, "{}", url).unwrap();
        }),
    );

    // --- verify phase ---
    let mut tasks = tokio::task::JoinSet::new();
    let mut processed = 0usize;
    let mut commitment_num = range.start;

    while let Some(result) = futures::StreamExt::next(&mut stream).await {
        // Respect the end bound, if any.
        if let Some(end) = range.end {
            if processed >= end - range.start {
                break;
            }
        }

        let chunk = match result {
            Ok(c) => c,
            Err(e) => {
                while tasks.join_next().await.is_some() {}
                return Err(e);
            }
        };

        let hex = if chunk.len() == 44 && chunk[36..].iter().all(|&b| b == 0) {
            hex::encode(&chunk[..36])
        } else {
            hex::encode(&chunk)
        };

        // First 4 bytes are a big-endian Unix timestamp.
        let timestamp_str = commitment_time(&chunk);

        // Decode hex -> raw bytes for the OTS start_digest check.
        let commitment_bytes = match hex::decode(&hex) {
            Ok(b) => b,
            Err(_) => {
                processed += 1;
                commitment_num += 1;
                continue;
            }
        };

        let url = base_url
            .join(&format!("timestamp/{}", hex))
            .map_err(AppError::Url)?;

        let client = client.clone();
        let ok = ok.clone();
        let errors = errors.clone();
        let missing = missing.clone();
        let verified = verified.clone();
        let chain_unverified = chain_unverified.clone();
        let pending = pending.clone();
        let pb = pb.clone();
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let rt = rate_tracker.clone();
        let url_update = latest_url.clone();

        // Clone the shared Bitcoin client reference into the task.
        let btc_arc = btc.clone();

        tasks.spawn(async move {
            let _permit = permit;
            if verbose {
                println!("GET {}", url);
            }
            let url_for_log = url.clone();
            *url_update.lock().unwrap() = url_for_log.to_string();

            let result = crate::verify::verify_timestamp_against_node(
                &client,
                url,
                &commitment_bytes,
                btc_arc,
            )
            .await;

            rt.record();
            match result {
                VerifyResult::Verified { height } => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    verified.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... verified (block {})",
                        commitment_num, timestamp_str, height
                    ));
                }
                VerifyResult::ChainUnverified { height } => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    chain_unverified.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... chain-unverified (block {})",
                        commitment_num, timestamp_str, height
                    ));
                }
                VerifyResult::Pending => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    pending.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... pending",
                        commitment_num, timestamp_str
                    ));
                }
                VerifyResult::Unverified { reason } => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... unverified ({})",
                        commitment_num, timestamp_str, reason
                    ));
                    eprintln!(
                        "\n  UNVERIFIED: commitment {} ({}): {}",
                        commitment_num, url_for_log, reason
                    );
                }
                VerifyResult::Missing => {
                    missing.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... MISSING",
                        commitment_num, timestamp_str
                    ));
                    eprintln!("\n  MISSING: commitment {} ({})", commitment_num, url_for_log);
                }
                VerifyResult::HttpError(e) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... ERROR",
                        commitment_num, timestamp_str
                    ));
                    eprintln!(
                        "\n  ERROR fetching commitment {} ({}): {}",
                        commitment_num, url_for_log, e
                    );
                }
                VerifyResult::ParseError(e) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    pb.set_message(format!(
                        "commitment {} @ {} ... PARSE ERROR",
                        commitment_num, timestamp_str
                    ));
                    eprintln!(
                        "\n  PARSE ERROR commitment {} ({}): {}",
                        commitment_num, url_for_log, e
                    );
                }
            }
            pb.inc(1);
        });

        processed += 1;
        commitment_num += 1;
    }

    // Wait for all in-flight requests.
    while tasks.join_next().await.is_some() {}

    Ok(Stats {
        ok: ok.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        missing: missing.load(Ordering::Relaxed),
        verified: verified.load(Ordering::Relaxed),
        chain_unverified: chain_unverified.load(Ordering::Relaxed),
        pending: pending.load(Ordering::Relaxed),
    })
}

/// Extract the first 4 bytes as a big-endian Unix timestamp and format it.
fn commitment_time(chunk: &[u8]) -> String {
    if chunk.len() < 4 {
        return "?".to_string();
    }
    let secs = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    match chrono::DateTime::from_timestamp(secs as i64, 0) {
        Some(t) => t.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => "?".to_string(),
    }
}
