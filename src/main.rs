use bitcoincore_rpc::RpcApi;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::io;
use thiserror::Error;
use url::Url;

mod journal;
mod range;
mod verifier;
mod verify;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("URL error: {0}")]
    Url(#[from] url::ParseError),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid calendar URL: {0}")]
    InvalidCalendarUrl(String),
    #[error("Download was cancelled")]
    Cancelled,
    #[error("Bitcoin RPC error: {0}")]
    BitcoinRpc(String),
}

/// Verify an OpenTimestamps calendar by checking that every commitment in its
/// journal can be downloaded as an individual timestamp.
#[derive(Parser, Debug)]
#[command(about = "Verify an OpenTimestamps calendar's journal commitments")]
struct Args {
    /// Calendar base URL (e.g. https://alice.btc.calendar.opentimestamps.org)
    #[arg(value_name = "URL")]
    calendar_url: String,

    /// Maximum number of concurrent HTTP requests
    #[arg(short, long, default_value = "20")]
    concurrency: usize,

    /// Print each timestamp URL as it is fetched
    #[arg(short, long)]
    verbose: bool,

    /// Range of journal commitments to verify (Rust range syntax: 5.., ..10, 3..7, 0..=9)
    #[arg(short, long, default_value = "..")]
    range: range::EntryRange,

    /// Enable Bitcoin verification using the local node (defaults:
    /// http://localhost:8332 with ~/.bitcoin/.cookie)
    #[arg(long)]
    btc: bool,

    /// Bitcoin RPC URL [default: http://localhost:8332]
    #[arg(long, value_name = "URL")]
    bitcoin_rpc: Option<String>,

    /// Bitcoin RPC user name (alternative to cookie auth)
    #[arg(long, value_name = "USER")]
    bitcoin_rpc_user: Option<String>,

    /// Bitcoin RPC password (alternative to cookie auth)
    #[arg(long, value_name = "PASS")]
    bitcoin_rpc_pass: Option<String>,

    /// Bitcoin RPC cookie file path [default: ~/.bitcoin/.cookie]
    #[arg(long, value_name = "PATH")]
    bitcoin_rpc_cookie: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let args = Args::parse();

    let base = Url::parse(&args.calendar_url)?;
    let journal_url = base.join("journal")?;

    println!("Fetching journal from: {}", journal_url);
    println!("Verifying range: {}", args.range);

    if let Some(ref url) = args.bitcoin_rpc {
        println!("Bitcoin RPC: {}", url);
    }

    let client = reqwest::Client::builder()
        .user_agent(format!(
            "Temporal-Integrity-Commission-v{}",
            env!("CARGO_PKG_VERSION")
        ))
        .build()?;

    // Optionally connect to a local Bitcoin node.
    let btc: Option<std::sync::Arc<bitcoincore_rpc::Client>> = if args.btc {
        let url = args
            .bitcoin_rpc
            .as_deref()
            .unwrap_or("http://localhost:8332");
        let cookie = args
            .bitcoin_rpc_cookie
            .as_deref()
            .map(|p| p.to_string())
            .or_else(|| {
                home::home_dir().map(|mut p| {
                    p.push(".bitcoin/.cookie");
                    p.to_string_lossy().into_owned()
                })
            });
        let client = verify::connect_btc(
            url,
            args.bitcoin_rpc_user.as_deref(),
            args.bitcoin_rpc_pass.as_deref(),
            cookie.as_deref(),
        )
        .map_err(|e| {
            eprintln!("ERROR: failed to connect to Bitcoin RPC: {}", e);
            AppError::BitcoinRpc(e.to_string())
        })?;
        println!(
            "Bitcoin node connected (height: {})",
            client.get_block_count().unwrap_or(0)
        );
        Some(std::sync::Arc::new(client))
    } else {
        None
    };

    // Stream-download the journal so we don't have to buffer the whole thing.
    let stream = journal::journal_commitments(client.clone(), journal_url, args.range.start);

    // Set up a progress bar.  We don't know the total number of commitments
    // in advance, so we show per-chunk progress.
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{elapsed_precise}] {pos} commitments verified ({msg})",
        )
        .unwrap(),
    );
    pb.set_message("starting...");

    let result = verifier::verify_chunks(
        &client,
        base,
        stream,
        &pb,
        args.concurrency,
        args.verbose,
        args.range,
        btc,
    )
    .await;

    match &result {
        Ok(stats) => {
            pb.finish_with_message(format!(
                "done. {} ok, {} errors, {} missing",
                stats.ok, stats.errors, stats.missing
            ));
            println!();
            println!("Verification complete:");
            println!("  Successful:     {}", stats.ok);
            println!("    Cryptographically verified: {}", stats.verified);
            println!(
                "    Chain-unverified (no node): {}",
                stats.chain_unverified
            );
            println!("    Pending (not yet mined):    {}", stats.pending);
            println!("  Failed:         {}", stats.errors);
            println!("  Missing:        {}", stats.missing);
        }
        Err(e) => {
            pb.abandon_with_message(format!("error: {}", e));
        }
    }

    result?;
    Ok(())
}
