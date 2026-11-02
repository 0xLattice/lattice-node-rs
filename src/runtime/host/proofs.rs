//! CID-authenticated dictionary witnesses anchored to this host's executed tip.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_cashew::{ContentSource, VolumeMerkleDictionary};
use lattice_primitives::{DepositKey, LatticeState, ReceiptKey};
use std::cell::RefCell;
struct Collector<'a> {
    source: &'a NodeStorage,
    entries: RefCell<BTreeMap<String, Vec<u8>>>,
}
impl ContentSource for Collector<'_> {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        let bytes = self.source.fetch(cid)?;
        let mut entries = self.entries.borrow_mut();
        if bytes.len() > 4 * 1024 * 1024
            || entries
                .values()
                .map(Vec::len)
                .sum::<usize>()
                .saturating_add(bytes.len())
                > 64 * 1024 * 1024
        {
            return Err(lattice_cashew::Error::Source(
                "proof exceeds local budget".into(),
            ));
        }
        entries.insert(cid.into(), bytes.clone());
        Ok(bytes)
    }
}
fn deposit_key(key: &str) -> Option<DepositKey> {
    let parts: Vec<_> = key.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() < 3 || parts[0].is_empty() {
        return None;
    }
    Some(DepositKey {
        demander: parts[0].into(),
        amount_demanded: parts[1].parse().ok()?,
        nonce: parts[2].parse().ok()?,
    })
}
impl Host {
    pub(super) fn dictionary_read(
        &self,
        path: &str,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Value>> {
        let requested = q.get("chainPath").map(String::as_str).unwrap_or("Nexus");
        let mut chain_path = requested
            .split('/')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if path == "/api/receipt-state" {
            if chain_path.len() < 2 {
                return Ok(None);
            }
            chain_path.pop();
        }
        let (graph, tip) = if chain_path == ["Nexus"] {
            (self.chain.tree(), self.mining.state().tip_cid())
        } else {
            let Some(core) = self.levels.get(&chain_path) else {
                return Ok(None);
            };
            let Some(mining) = self.child_mining.get(&chain_path) else {
                return Ok(None);
            };
            (core.tree(), mining.core.state().tip_cid())
        };
        let block = self.block(tip)?;
        let state = block.post_state.0.resolve(self.storage.as_ref(), None)?;
        let state = state.node.as_ref().unwrap();
        match path {
            "/api/deposit-state" => {
                let key = q
                    .get("key")
                    .filter(|k| deposit_key(k).is_some())
                    .ok_or_else(|| anyhow::anyhow!("invalid deposit key"))?;
                let (proof, values) = self.dictionary_proof::<u64>(
                    &block,
                    "deposits",
                    &state.deposit_state,
                    std::slice::from_ref(key),
                )?;
                let value = values.get(key).cloned().flatten();
                let mut response = json!({"exists":value.is_some(),"key":key,"chain":chain_path.join("/"),"proof":proof});
                if let Some(value) = value {
                    response["value"] = json!(value);
                }
                Ok(Some(response))
            }
            "/api/receipt-state" => {
                let destination = q
                    .get("chainPath")
                    .ok_or_else(|| anyhow::anyhow!("missing chain path"))?;
                let parts: Vec<String> = destination.split('/').map(str::to_owned).collect();
                if parts.len() != chain_path.len() + 1
                    || parts[..parts.len() - 1] != chain_path
                    || lattice_primitives::ChainRuntimeContext::new(parts.clone(), None).is_err()
                {
                    return Ok(None);
                }
                let key = ReceiptKey {
                    directory: parts.last().unwrap().clone(),
                    demander: q
                        .get("demander")
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| anyhow::anyhow!("missing demander"))?
                        .clone(),
                    amount_demanded: q
                        .get("amount")
                        .ok_or_else(|| anyhow::anyhow!("missing amount"))?
                        .parse()?,
                    nonce: q
                        .get("nonce")
                        .ok_or_else(|| anyhow::anyhow!("missing nonce"))?
                        .parse()?,
                };
                let (proof, values) = self.dictionary_proof::<String>(
                    &block,
                    "receipts",
                    &state.receipt_state,
                    &[key.storage_key()],
                )?;
                let value = values.get(&key.storage_key()).cloned().flatten();
                let mut response = json!({"exists":value.is_some(),"directory":key.directory,"chainPath":parts,"key":key.to_string(),"proof":proof});
                if let Some(value) = value {
                    response["withdrawer"] = json!(value);
                }
                Ok(Some(response))
            }
            "/api/deposits" => {
                let limit = q
                    .get("limit")
                    .map(|s| s.parse::<usize>())
                    .transpose()?
                    .unwrap_or(100)
                    .min(100);
                let after = q.get("after").map(String::as_str);
                let root = state.deposit_state.0.resolve(self.storage.as_ref(), None)?;
                let scanned = root.node.as_ref().unwrap().bounded_keys_and_values(
                    after,
                    limit + 1,
                    self.storage.as_ref(),
                )?;
                let more = scanned.len() > limit;
                let entries: Vec<_> = scanned.into_iter().take(limit).collect();
                let keys = entries.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
                let (proof, _) =
                    self.dictionary_proof::<u64>(&block, "deposits", &state.deposit_state, &keys)?;
                let mut deposits = vec![];
                for (raw, amount) in &entries {
                    if *amount == 0 {
                        continue;
                    }
                    let Some(key) = deposit_key(raw) else {
                        continue;
                    };
                    let mut row = json!({"key":raw,"demander":key.demander,"amountDemanded":key.amount_demanded.to_string(),"nonce":key.nonce.to_string(),"amountDeposited":amount.to_string()});
                    if let Some((height, cid)) =
                        self.deposit_inclusion(raw, block.height, graph, tip)?
                    {
                        row["blockHeight"] = json!(height.to_string());
                        row["blockHash"] = json!(cid);
                    }
                    deposits.push(row);
                }
                let mut response = json!({"count":deposits.len(),"deposits":deposits,"chain":chain_path.join("/"),"proof":proof});
                if more && let Some((key, _)) = entries.last() {
                    response["next"] = json!(key);
                }
                Ok(Some(response))
            }
            _ => Ok(None),
        }
    }
    fn dictionary_proof<V: Clone + serde::Serialize + serde::de::DeserializeOwned + ToString>(
        &self,
        block: &Block,
        kind: &str,
        dictionary: &Volume<VolumeMerkleDictionary<V>>,
        keys: &[String],
    ) -> anyhow::Result<(Value, BTreeMap<String, Option<String>>)> {
        let collector = Collector {
            source: self.storage.as_ref(),
            entries: RefCell::new(BTreeMap::new()),
        };
        let _ = block.post_state.0.resolve(&collector, None)?;
        let root = dictionary.0.resolve(&collector, None)?;
        let mut tree = root.node.as_ref().unwrap().as_ref().clone();
        let mut values = BTreeMap::new();
        let mut claims = vec![];
        for key in keys {
            tree = tree.resolve_key(key, &collector)?;
            let value = tree.get(key)?.map(ToString::to_string);
            let mut claim = json!({"key":key});
            if let Some(value) = &value {
                claim["value"] = json!(value);
            }
            claims.push(claim);
            values.insert(key.clone(), value);
        }
        let cid = content_id(block)?;
        let block_bytes = self.storage.fetch(&cid)?;
        let witness = collector
            .entries
            .into_inner()
            .into_iter()
            .map(|(cid, data)| json!({"cid":cid,"data":STANDARD.encode(data)}))
            .collect::<Vec<_>>();
        Ok((
            json!({"blockHash":cid,"blockHeight":block.height,"block":{"cid":cid,"data":STANDARD.encode(block_bytes)},"stateRoot":block.post_state.0.raw_cid,"dictionary":kind,"dictionaryRoot":dictionary.0.raw_cid,"claims":claims,"witness":witness}),
            values,
        ))
    }
    fn deposit_inclusion(
        &self,
        key: &str,
        tip_height: u64,
        graph: &lattice_block_tree::ChainTree,
        tip: &str,
    ) -> anyhow::Result<Option<(u64, String)>> {
        let canonical_at = |height: u64| -> Option<String> {
            let mut at = tip;
            loop {
                let h = graph.height(at)?;
                if h == height {
                    return Some(at.into());
                }
                if h < height {
                    return None;
                }
                at = graph.parent(at)?;
            }
        };
        let mut low = 0;
        let mut high = tip_height;
        while low < high {
            let middle = low + (high - low) / 2;
            let Some(cid) = canonical_at(middle) else {
                return Ok(None);
            };
            let block = self.block(&cid)?;
            let state = block.post_state.0.resolve(self.storage.as_ref(), None)?;
            let state: &LatticeState = state.node.as_ref().unwrap();
            let deposits = state.deposit_state.0.resolve(self.storage.as_ref(), None)?;
            let tree = deposits
                .node
                .as_ref()
                .unwrap()
                .resolve_key(key, self.storage.as_ref())?;
            if tree.get(key)?.is_some() {
                high = middle
            } else {
                low = middle + 1
            }
        }
        Ok(canonical_at(high).map(|cid| (high, cid)))
    }
}
