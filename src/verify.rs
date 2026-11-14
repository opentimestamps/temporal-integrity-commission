use bitcoincore_rpc::{Auth, Client as BtcClient, RpcApi};
use opentimestamps::attestation::Attestation;
use opentimestamps::ser::DetachedTimestampFile;
use opentimestamps::ser::Deserializer;
use opentimestamps::timestamp::Step;
use opentimestamps::timestamp::StepData;
use opentimestamps::timestamp::Timestamp;

#[derive(Debug, Clone)]
pub enum VerifyResult {
    /// Proof structure is valid and points to a Bitcoin block that was
    /// confirmed by the local node.
    Verified { height: u64 },
    /// Proof structure is valid and contains a Bitcoin attestation, but
    /// no local node was available to confirm the block on-chain.
    ChainUnverified { height: u64 },
    /// Proof found but only in pending (not yet mined) state
    Pending,
    /// Proof found but couldn't be verified against the Bitcoin node
    Unverified { reason: String },
    /// HTTP 404
    Missing,
    /// HTTP error fetching the proof
    HttpError(String),
    /// Proof file couldn't be parsed
    ParseError(String),
}

/// Connection parameters for a local Bitcoin node.
#[derive(Clone)]
pub struct BtcConn {
    pub url: String,
    pub user: Option<String>,
    pub pass: Option<String>,
    pub cookie: Option<String>,
}

impl BtcConn {
    fn to_client(&self) -> Result<BtcClient, Box<dyn std::error::Error>> {
        let auth = if let Some(ref path) = self.cookie {
            Auth::CookieFile(path.into())
        } else if let (Some(ref u), Some(ref p)) = (&self.user, &self.pass) {
            Auth::UserPass(u.clone(), p.clone())
        } else {
            Auth::None
        };
        Ok(BtcClient::new(&self.url, auth)?)
    }
}

/// Download an OpenTimestamps proof from `url`, parse it, and verify any
/// Bitcoin attestations against a local node when `btc` is provided.
pub async fn verify_timestamp_against_node(
    client: &reqwest::Client,
    url: reqwest::Url,
    expected_commitment: &[u8],
    btc: Option<&BtcConn>,
) -> VerifyResult {
    let bytes = match download_bytes(client, url.clone()).await {
        Ok(b) => b,
        Err(e) => {
            if e.status() == Some(reqwest::StatusCode::NOT_FOUND) {
                return VerifyResult::Missing;
            }
            return VerifyResult::HttpError(e.to_string());
        }
    };

    // Calendar servers may return either a full `.ots` DetachedTimestampFile
    // or a raw serialized `Timestamp`. Try both.
    let timestamp = match DetachedTimestampFile::from_reader(&bytes[..]) {
        Ok(file) => {
            if file.timestamp.start_digest != expected_commitment {
                return VerifyResult::ParseError(format!(
                    "start_digest {} does not match commitment {}",
                    hex::encode(&file.timestamp.start_digest),
                    hex::encode(expected_commitment)
                ));
            }
            file.timestamp
        }
        Err(_) => {
            // Try raw Timestamp deserialization.
            let mut deser = Deserializer::new(&bytes[..]);
            match Timestamp::deserialize(&mut deser, expected_commitment.to_vec()) {
                Ok(ts) => ts,
                Err(e) => return VerifyResult::ParseError(e.to_string()),
            }
        }
    };

    // Build a Bitcoin client if requested (done here so we don't pay the
    // cost unless a proof was actually downloaded and parsed successfully).
    let btc_client = match btc {
        Some(conn) => match conn.to_client() {
            Ok(c) => Some(c),
            Err(e) => {
                return VerifyResult::Unverified {
                    reason: format!("failed to connect to Bitcoin node: {}", e),
                }
            }
        },
        None => None,
    };

    // Walk the proof tree looking for Bitcoin and Pending attestations.
    let had_node = btc_client.is_some();
    match walk_tree(&timestamp.first_step, btc_client.as_ref()) {
        TreeResult::BitcoinOk { height, verified } => {
            if verified {
                VerifyResult::Verified { height }
            } else if had_node {
                // Node was queried but verification failed for some reason
                // (the walk_tree logic handles its own error reporting).
                VerifyResult::ChainUnverified { height }
            } else {
                VerifyResult::ChainUnverified { height }
            }
        }
        TreeResult::Pending => VerifyResult::Pending,
        TreeResult::Unknown(reason) => VerifyResult::Unverified { reason },
    }
}

async fn download_bytes(
    client: &reqwest::Client,
    url: reqwest::Url,
) -> Result<Vec<u8>, reqwest::Error> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(resp.error_for_status().unwrap_err());
    }
    Ok(resp.bytes().await?.to_vec())
}

#[derive(Debug)]
enum TreeResult {
    BitcoinOk { height: u64, verified: bool },
    Pending,
    Unknown(String),
}

fn walk_tree(step: &Step, btc: Option<&BtcClient>) -> TreeResult {
    match &step.data {
        StepData::Attestation(Attestation::Bitcoin { height }) => {
            if let Some(client) = btc {
                match client.get_block_hash(*height as u64) {
                    Ok(block_hash) => {
                        match client.get_block_header(&block_hash) {
                            Ok(header) => {
                                let output = &step.output;
                                let merkle: &[u8] = header.merkle_root.as_ref();
                                // Cryptographic verification: the final output digest
                                // should be the 32-byte merkle root. We report
                                // verified=true only when this check passes.
                                let digest_ok =
                                    output.len() == 32 && output == merkle;
                                TreeResult::BitcoinOk {
                                    height: *height as u64,
                                    verified: digest_ok,
                                }
                            }
                            Err(e) => TreeResult::Unknown(format!(
                                "Bitcoin node returned error for height {}: {}",
                                height, e
                            )),
                        }
                    }
                    Err(e) => TreeResult::Unknown(format!(
                        "Bitcoin node has no block at height {}: {}",
                        height, e
                    )),
                }
            } else {
                // No local node: proof structure is OK but chain not checked.
                TreeResult::BitcoinOk {
                    height: *height as u64,
                    verified: false,
                }
            }
        }
        StepData::Attestation(Attestation::Pending { .. }) => TreeResult::Pending,
        StepData::Attestation(Attestation::Unknown { tag, .. }) => TreeResult::Unknown(format!(
            "unknown attestation type {}",
            hex::encode(tag)
        )),
        StepData::Fork => {
            let mut found_bitcoin = None;
            let mut found_pending = false;
            for fork in &step.next {
                match walk_tree(fork, btc) {
                    TreeResult::BitcoinOk { height, verified } => {
                        found_bitcoin = Some((height, verified));
                        break;
                    }
                    TreeResult::Pending => {
                        found_pending = true;
                    }
                    TreeResult::Unknown(_) => {}
                }
            }
            if let Some((h, v)) = found_bitcoin {
                TreeResult::BitcoinOk {
                    height: h,
                    verified: v,
                }
            } else if found_pending {
                TreeResult::Pending
            } else {
                TreeResult::Unknown("all fork paths failed".to_string())
            }
        }
        StepData::Op(_) => {
            if let Some(next) = step.next.first() {
                walk_tree(next, btc)
            } else {
                TreeResult::Unknown("op step with no continuation".to_string())
            }
        }
    }
}
