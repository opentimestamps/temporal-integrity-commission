use reqwest::{RequestBuilder, StatusCode};
use std::time::Duration;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(600);

/// Send `req` with a timeout, retrying with exponential backoff on
/// transient failures.  A countdown is printed to stderr during each
/// backoff period.
pub async fn fetch_with_backoff(
    req: RequestBuilder,
    label: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let attempt = req.try_clone().expect("request must be cloneable");
        match attempt.timeout(REQUEST_TIMEOUT).send().await {
            Ok(resp) => {
                if resp.status().is_server_error()
                    && resp.status() != StatusCode::RANGE_NOT_SATISFIABLE
                {
                    // 5xx are transient; retry them.
                    let status = resp.status();
                    eprintln!(
                        "{} returned {}, retrying in {:?}...",
                        label, status, backoff
                    );
                    countdown_sleep(backoff).await;
                    if backoff < MAX_BACKOFF {
                        backoff *= 2;
                    }
                    continue;
                }
                return Ok(resp);
            }
            Err(e) => {
                if e.is_timeout() || e.is_connect() || is_transient(&e) {
                    eprintln!(
                        "{} failed ({}), retrying in {:?}...",
                        label, e, backoff
                    );
                    countdown_sleep(backoff).await;
                    if backoff < MAX_BACKOFF {
                        backoff *= 2;
                    }
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// Fetch the body of `req` as raw bytes, with the same retry behaviour.
pub async fn fetch_bytes_with_backoff(
    req: RequestBuilder,
    label: &str,
) -> Result<Vec<u8>, reqwest::Error> {
    let resp = fetch_with_backoff(req, label).await?;
    if !resp.status().is_success() {
        return Err(resp.error_for_status().unwrap_err());
    }
    Ok(resp.bytes().await?.to_vec())
}

/// Sleep for `duration`, printing a countdown to stderr every second.
async fn countdown_sleep(duration: Duration) {
    let secs = duration.as_secs();
    if secs == 0 {
        tokio::time::sleep(duration).await;
        return;
    }
    let start = std::time::Instant::now();
    for remaining in (1..=secs).rev() {
        eprint!("  {}s... ", remaining);
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let elapsed = start.elapsed();
    if elapsed < duration {
        tokio::time::sleep(duration - elapsed).await;
    }
    eprintln!();
}

fn is_transient(e: &reqwest::Error) -> bool {
    let text = e.to_string().to_lowercase();
    text.contains("incompletebody")
        || text.contains("connection reset")
        || text.contains("connection aborted")
        || text.contains("broken pipe")
        || text.contains("unexpected eof")
}
