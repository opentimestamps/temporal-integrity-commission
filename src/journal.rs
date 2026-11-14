use reqwest::{Client, StatusCode};

use crate::AppError;

const COMMITMENT_SIZE: usize = 44;

/// Returns a stream of 44-byte journal commitments with automatic retry
/// on interrupted body streams (e.g. IncompleteBody).
///
/// A background task drives the HTTP download and retry loop; the returned
/// receiver is a plain `Unpin` stream that the caller can consume.
pub fn journal_commitments(
    client: Client,
    url: reqwest::Url,
    skip_commitments: usize,
) -> impl futures::Stream<Item = Result<Vec<u8>, AppError>> + Send + Unpin {
    use futures::channel::mpsc;
    use futures::{SinkExt, StreamExt};

    let (mut tx, rx) = mpsc::channel(64);

    tokio::spawn(async move {
        let mut byte_offset = match skip_commitments.checked_mul(COMMITMENT_SIZE) {
            Some(n) => n,
            None => {
                let _ = tx
                    .send(Err(AppError::InvalidCalendarUrl(format!(
                        "skip_commitments {} too large",
                        skip_commitments
                    ))))
                    .await;
                return;
            }
        };
        let mut buffer = Vec::with_capacity(8192);

        if byte_offset > 0 {
            println!(
                "Server supports range requests; jumped to commitment {} (byte {})",
                skip_commitments, byte_offset
            );
        }

        loop {
            let req = client.get(url.clone());
            let req = if byte_offset > 0 {
                req.header(reqwest::header::RANGE, format!("bytes={}-", byte_offset))
            } else {
                req
            };

            let label = format!("GET journal from {}", url);
            let resp = match crate::http::fetch_with_backoff(req, &label).await {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(AppError::Http(e))).await;
                    return;
                }
            };

            let status = resp.status();

            if byte_offset > 0 {
                if status == StatusCode::PARTIAL_CONTENT {
                    // Range request succeeded.
                } else if status == StatusCode::RANGE_NOT_SATISFIABLE {
                    // Journal shrank since our last request.
                    if !buffer.is_empty() {
                        let _ = tx
                            .send(Err(AppError::Io(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!("journal ended with {} bytes remaining", buffer.len()),
                            ))))
                            .await;
                    }
                    return;
                } else if !status.is_success() {
                    let _ = tx
                        .send(Err(AppError::Http(resp.error_for_status().unwrap_err())))
                        .await;
                    return;
                }
            } else if !status.is_success() {
                let _ = tx
                    .send(Err(AppError::Http(resp.error_for_status().unwrap_err())))
                    .await;
                return;
            }

            let mut body = resp.bytes_stream();
            let mut stream_broken = false;

            while let Some(result) = body.next().await {
                match result {
                    Ok(bytes) => {
                        if bytes.is_empty() {
                            continue;
                        }
                        buffer.extend_from_slice(&bytes);
                        byte_offset += bytes.len();

                        while buffer.len() >= COMMITMENT_SIZE {
                            let chunk = buffer[..COMMITMENT_SIZE].to_vec();
                            buffer.copy_within(COMMITMENT_SIZE.., 0);
                            buffer.truncate(buffer.len() - COMMITMENT_SIZE);

                            if tx.send(Ok(chunk)).await.is_err() {
                                return; // Receiver dropped
                            }
                        }
                    }
                    Err(_e) => {
                        eprintln!(
                            "Journal stream interrupted from byte {}, \
                             resuming with backoff...",
                            byte_offset
                        );
                        stream_broken = true;
                        break;
                    }
                }
            }

            if !stream_broken {
                // Clean EOF.
                if !buffer.is_empty() {
                    let _ = tx
                        .send(Err(AppError::Io(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            format!(
                                "journal ended with {} bytes remaining (not a multiple of {})",
                                buffer.len(),
                                COMMITMENT_SIZE
                            ),
                        ))))
                        .await;
                }
                return;
            }
            // Otherwise loop and retry with updated byte_offset.
            // The outer loop will issue a new HTTP request starting from
            // byte_offset, and fetch_with_backoff handles retry/backoff.
        }
    });

    rx
}
