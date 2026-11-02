//! Root reads are gated by durable graph execution where they attest state.
//! Trie reads resolve requested paths; pagination never materializes all values.
use super::*;
use lattice_cashew::ContentSource;
struct ReadSource<'a>(&'a NodeStorage);
impl ContentSource for ReadSource<'_> {
    fn fetch(&self, cid: &str) -> Result<Vec<u8>, lattice_cashew::Error> {
        let bytes = self.0.fetch(cid)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(lattice_cashew::Error::Source(
                "read node exceeds limit".into(),
            ));
        }
        Ok(bytes)
    }
}
fn missing() -> (u16, Value) {
    (404, json!({"error":"not found"}))
}
fn optional(value: &mut Value, key: &str, input: Option<Value>) {
    if let Some(input) = input {
        value[key] = input;
    }
}
impl Host {
    pub(super) fn read(&self, route: &str) -> Option<(u16, Value)> {
        let (path, query) = route.split_once('?').unwrap_or((route, ""));
        if !(path == "/status" && !query.is_empty()
            || path.starts_with("/api/")
            || path.starts_with("/accounts/")
            || path.starts_with("/transactions/"))
        {
            return None;
        }
        let mut params = BTreeMap::new();
        for (k, v) in form_urlencoded::parse(query.as_bytes()) {
            params.entry(k.into_owned()).or_insert(v.into_owned());
        }
        if path != "/api/receipt-state" && params.get("chainPath").is_some_and(|p| p != "Nexus") {
            let chain = params["chainPath"]
                .split('/')
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            return Some(match self.child_read(path, &params, &chain) {
                Ok(Some(v)) => (200, v),
                Ok(None) => missing(),
                Err(_) => missing(),
            });
        }
        let result = self.read_value(path, &params);
        Some(match result {
            Ok(Some(value)) => (200, value),
            Ok(None) => missing(),
            Err(e) if e.downcast_ref::<lattice_cashew::Error>().is_some() => missing(),
            Err(e) => error(e),
        })
    }
    fn read_value(
        &self,
        path: &str,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Value>> {
        let source = ReadSource(self.storage.as_ref());
        let tip = self.mining.state().tip_cid();
        let block = |cid: &str| -> anyhow::Result<Option<Block>> {
            if !self.chain.tree().contains(cid) {
                return Ok(None);
            }
            Ok(Header::<Block>::from_cid(cid)?
                .resolve(&source, None)
                .ok()
                .and_then(|h| h.node.as_deref().cloned()))
        };
        let state_account = |owner: &str, cid: &str| -> anyhow::Result<Option<(u64, u64)>> {
            if !self.chain.tree().is_executed(cid) {
                return Ok(None);
            }
            let Some(block) = block(cid)? else {
                return Ok(None);
            };
            let state = block.post_state.0.resolve(&source, None)?;
            let accounts = state
                .node
                .as_ref()
                .unwrap()
                .account_state
                .0
                .resolve(&source, None)?;
            let nonce_key = format!("{}{owner}", lattice_validation::NONCE_PREFIX);
            let tree = accounts
                .node
                .as_ref()
                .unwrap()
                .resolve_key(owner, &source)?
                .resolve_key(&nonce_key, &source)?;
            let balance = tree.get(owner)?.copied().unwrap_or(0);
            let nonce = lattice_primitives::next_expected_nonce(tree.get(&nonce_key)?.copied())?;
            Ok(Some((balance, nonce)))
        };
        match path {
            "/api/deposits" | "/api/deposit-state" | "/api/receipt-state" => {
                self.dictionary_read(path, q)
            }
            "/api/peers" => {
                let peers = self
                    .network
                    .peer_summaries()
                    .into_iter()
                    .map(|(key, role)| json!({"key":key,"role":role}))
                    .collect::<Vec<_>>();
                Ok(Some(json!({"count":peers.len(),"peers":peers})))
            }
            "/api/chain/children" => {
                let n = limit(q, 200, 200)?;
                let after = q.get("after");
                let names = self
                    .executed_children
                    .iter()
                    .filter(|name| after.is_none_or(|after| *name > after))
                    .take(n.saturating_add(1))
                    .cloned()
                    .collect::<Vec<_>>();
                let mut value = json!({"chainPath":["Nexus"],"directories":names.iter().take(n).collect::<Vec<_>>()});
                if names.len() > n && n > 0 {
                    value["next"] = json!(names[n - 1]);
                }
                Ok(Some(value))
            }
            "/api/chain/info" => Ok(Some(
                json!({"chain":["Nexus"],"genesisHash":self.genesis,"height":self.chain.tree().height(tip).unwrap_or(0).to_string(),"tipCID":tip,"minRelayFee":self.mining.state().mempool().limits().min_relay_fee.to_string(),"acceptsSubmit":true}),
            )),
            "/api/chain/genesis" => Ok(Some(json!({"genesisHash":self.genesis}))),
            "/api/chain/spec" | "/api/fees" => {
                let Some(block) = block(tip)? else {
                    return Ok(None);
                };
                let spec = block.spec.0.resolve(&source, None)?;
                let s = spec.node.as_ref().unwrap();
                if path.ends_with("spec") {
                    return Ok(Some(
                        json!({"targetBlockTime":s.target_block_time.to_string(),"initialReward":s.initial_reward.to_string(),"halvingInterval":s.halving_interval.to_string(),"maxBlockSize":s.max_block_size,"maxNumberOfTransactionsPerBlock":s.max_number_of_transactions_per_block.to_string(),"premine":s.premine.to_string(),"halfLife":s.half_life.to_string()}),
                    ));
                }
                let weight = crate::core::mempool::FeeWeight::new(s);
                let blocks=self.mining.state().mempool().projected_blocks(12,s).into_iter().map(|p|json!({"transactionCount":p.transaction_count,"bytes":p.bytes,"lowestFee":num_bigint::BigUint::parse_bytes(p.lowest_fee.to_hex().as_bytes(),16).unwrap().min(num_bigint::BigUint::from(u64::MAX)).to_string(),"lowestFeeBytes":p.lowest_fee_bytes,"lowestFeeWeight":p.lowest_fee_weight,"full":p.full})).collect::<Vec<_>>();
                Ok(Some(
                    json!({"minRelayFee":self.mining.state().mempool().limits().min_relay_fee.to_string(),"blocks":blocks,"weight":{"blockRoom":weight.block_room,"growthRoom":weight.growth_room,"maxTransactions":weight.max_transactions,"transactionAllowance":weight.transaction_allowance}}),
                ))
            }
            "/api/mempool" => Ok(Some(
                json!({"count":self.mining.state().mempool().count(),"transactions":self.mining.state().mempool().items().into_iter().take(200).map(|i|i.cid).collect::<Vec<_>>()}),
            )),
            "/api/block/latest" => self.block_view(tip, false),
            "/api/blocks" => {
                let limit = limit(q, 10, 25)?;
                let height = self.chain.tree().height(tip).unwrap_or(0);
                let before = q
                    .get("before")
                    .map(|s| s.parse::<u64>())
                    .transpose()?
                    .unwrap_or(height.saturating_add(1))
                    .min(height.saturating_add(1));
                let mut current = before;
                let mut blocks = vec![];
                for _ in 0..limit {
                    if current == 0 {
                        break;
                    }
                    current -= 1;
                    if let Some(cid) = self.canonical_at(current)
                        && let Some(mut view) = self.block_view(&cid, false)?
                    {
                        view.as_object_mut().unwrap().remove("rewardCredited");
                        blocks.push(view);
                    }
                }
                let mut value = json!({"blocks":blocks});
                optional(
                    &mut value,
                    "nextBefore",
                    (current > 0).then(|| json!(current.to_string())),
                );
                Ok(Some(value))
            }
            p if p.starts_with("/accounts/") => {
                let owner = p.trim_start_matches("/accounts/");
                let cid = q
                    .get("block")
                    .ok_or_else(|| anyhow::anyhow!("missing block"))?;
                Ok(state_account(owner,cid)?.map(|(balance,nonce)|json!({"owner":owner,"block":cid,"balance":balance.to_string(),"nonce":nonce.to_string()})))
            }
            p if p.starts_with("/api/state/account/") => {
                let owner = p.trim_start_matches("/api/state/account/");
                Ok(state_account(owner,tip)?.map(|(balance,nonce)|json!({"owner":owner,"balance":balance.to_string(),"nonce":nonce.to_string()})))
            }
            p if p.starts_with("/api/transaction/") => {
                let cid = p.trim_start_matches("/api/transaction/");
                let tx = Header::<Transaction>::from_cid(cid)?.resolve(&source, None)?;
                let body = tx.node.as_ref().unwrap().body.resolve(&source, None)?;
                let body = body.node.as_ref().unwrap();
                let mut value = json!({"txCID":cid,"nonce":body.nonce.to_string(),"signers":body.signers,"chainPath":body.chain_path,"chain":body.chain_path,
                    "accountActions":body.account_actions.iter().map(|a|json!({"owner":a.owner,"delta":a.delta.to_string()})).collect::<Vec<_>>(),
                    "depositActions":body.deposit_actions.iter().map(|a|json!({"nonce":a.nonce.to_string(),"demander":a.demander,"amountDemanded":a.amount_demanded.to_string(),"amountDeposited":a.amount_deposited.to_string()})).collect::<Vec<_>>(),
                    "receiptActions":body.receipt_actions.iter().map(|a|json!({"withdrawer":a.withdrawer,"nonce":a.nonce.to_string(),"demander":a.demander,"amountDemanded":a.amount_demanded.to_string(),"directory":a.directory})).collect::<Vec<_>>(),
                    "withdrawalActions":body.withdrawal_actions.iter().map(|a|json!({"withdrawer":a.withdrawer,"nonce":a.nonce.to_string(),"demander":a.demander,"amountDemanded":a.amount_demanded.to_string(),"amountWithdrawn":a.amount_withdrawn.to_string()})).collect::<Vec<_>>()});
                // Missing inclusion evidence omits location; it never asserts
                // that a merely held transaction was canonically executed.
                if let Ok(Some((height, hash, timestamp))) = self.transaction_inclusion(cid, body) {
                    value["blockHeight"] = json!(height.to_string());
                    value["blockHash"] = json!(hash);
                    value["timestamp"] = json!(timestamp.to_string());
                }
                Ok(Some(value))
            }
            p if p.starts_with("/transactions/") => {
                let cid = p.trim_start_matches("/transactions/");
                let tx = Header::<Transaction>::from_cid(cid)?
                    .resolve(&source, None)
                    .ok()
                    .and_then(|h| h.node.as_deref().cloned());
                Ok(tx.map(|tx| json!({"cid":cid,"transaction":tx})))
            }
            p if p.starts_with("/api/block/") => {
                let id = p.trim_start_matches("/api/block/");
                if !id.contains('/') {
                    let cid = if let Ok(height) = id.parse::<u64>() {
                        let Some(cid) = self.canonical_at(height) else {
                            return Ok(None);
                        };
                        cid
                    } else {
                        id.into()
                    };
                    return self.block_view(&cid, true);
                }
                let (id, kind) = id.split_once('/').unwrap();
                let Some(block) = block(id)? else {
                    return Ok(None);
                };
                self.block_members(&block, kind, q)
            }
            _ => Ok(None),
        }
    }
    fn block_members(
        &self,
        block: &Block,
        kind: &str,
        q: &BTreeMap<String, String>,
    ) -> anyhow::Result<Option<Value>> {
        let source = ReadSource(self.storage.as_ref());
        match kind {
            "transactions" => {
                let offset = q
                    .get("offset")
                    .map(|s| s.parse::<usize>())
                    .transpose()?
                    .unwrap_or(0);
                let limit = limit(q, 20, 100)?;
                let root = block.transactions.resolve(&source, None)?;
                let total = root.node.as_ref().unwrap().count;
                let end = offset.saturating_add(limit).min(total);
                let mut transactions = vec![];
                for i in offset.min(total)..end {
                    let tree = root
                        .node
                        .as_ref()
                        .unwrap()
                        .resolve_key(&i.to_string(), &source)?;
                    if let Some(volume) = tree.get(&i.to_string())? {
                        let tx = volume.0.resolve(&source, None)?;
                        let body = tx.node.as_ref().unwrap().body.resolve(&source, None)?;
                        let b = body.node.as_ref().unwrap();
                        transactions.push(json!({"txCID":volume.0.raw_cid,"signers":b.signers,"accountActionCount":b.account_actions.len(),"depositActionCount":b.deposit_actions.len(),"receiptActionCount":b.receipt_actions.len(),"withdrawalActionCount":b.withdrawal_actions.len()}));
                    }
                }
                let mut value = json!({"transactions":transactions});
                optional(&mut value, "nextOffset", (end < total).then(|| json!(end)));
                Ok(Some(value))
            }
            "children" => {
                let root = block.children.resolve(&source, None)?;
                let mut children = vec![];
                for (directory, volume) in
                    root.node.as_ref().unwrap().iter().take(limit(q, 100, 100)?)
                {
                    let mut child = json!({"directory":directory,"blockHash":volume.0.raw_cid});
                    if let Ok(header) = volume.0.resolve(&source, None) {
                        let b = header.node.as_ref().unwrap();
                        child["height"] = json!(b.height.to_string());
                        child["transactionCount"] = json!(
                            b.transactions
                                .resolve(&source, None)
                                .ok()
                                .and_then(|h| h.node.as_ref().map(|d| d.count))
                                .unwrap_or(0)
                        );
                    }
                    children.push(child);
                }
                Ok(Some(json!({"children":children})))
            }
            _ => Ok(None),
        }
    }
    fn child_read(
        &self,
        route: &str,
        q: &BTreeMap<String, String>,
        path: &[String],
    ) -> anyhow::Result<Option<Value>> {
        let Some(core) = self.levels.get(path) else {
            return Ok(None);
        };
        let tree = core.tree();
        let mining = self.child_mining.get(path).map(|m| m.core.state());
        let Some(tip) = mining
            .map(|m| m.tip_cid())
            .or_else(|| tree.executed_prefix())
        else {
            if route == "/status" {
                let mut value = json!({"phase":"awaitingGenesis","chainPath":path,"nexusGenesisCID":self.genesis,"mempoolCount":0,"mempoolBytes":0});
                if !tree.canonical_tip().is_empty() {
                    value["bestHeaderHeight"] = json!(tree.highest_block_height().to_string());
                }
                return Ok(Some(value));
            }
            return Ok(None);
        };
        let source = ReadSource(self.storage.as_ref());
        let mut genesis = tip;
        while let Some(parent) = tree.parent(genesis) {
            genesis = parent;
        }
        match route {
            "/api/deposits" | "/api/deposit-state" => self.dictionary_read(route, q),
            "/status" => Ok(Some(
                json!({"phase":"active","chainPath":path,"nexusGenesisCID":self.genesis,"tipCID":tip,"height":tree.height(tip).unwrap_or(0).to_string(),"bestHeaderHeight":tree.highest_block_height().to_string(),"mempoolCount":mining.map(|m|m.mempool().count()).unwrap_or(0),"mempoolBytes":mining.map(|m|m.mempool().byte_count()).unwrap_or(0)}),
            )),
            "/api/chain/info" => Ok(Some(
                json!({"chain":path,"genesisHash":genesis,"height":tree.height(tip).unwrap_or(0).to_string(),"tipCID":tip,"minRelayFee":self.mining.state().mempool().limits().min_relay_fee.to_string(),"acceptsSubmit":mining.is_some()}),
            )),
            "/api/mempool" => Ok(Some(
                json!({"count":mining.map(|m|m.mempool().count()).unwrap_or(0),"transactions":mining.map(|m|m.mempool().items().into_iter().take(200).map(|i|i.cid).collect::<Vec<_>>()).unwrap_or_default()}),
            )),
            "/api/chain/children" => {
                let n = limit(q, 200, 200)?;
                let after = q.get("after");
                let names = self
                    .storage
                    .journal()
                    .executed_child_directories(path)?
                    .into_iter()
                    .filter(|name| after.is_none_or(|after| name > after))
                    .take(n.saturating_add(1))
                    .collect::<Vec<_>>();
                let mut value = json!({"chainPath":path,"directories":names.iter().take(n).collect::<Vec<_>>()});
                if names.len() > n && n > 0 {
                    value["next"] = json!(names[n - 1]);
                }
                Ok(Some(value))
            }
            "/api/peers" => {
                let keys = self
                    .network
                    .peers_hosting(path)
                    .into_iter()
                    .map(|p| p.key)
                    .collect::<BTreeSet<_>>();
                let peers = self
                    .network
                    .peer_summaries()
                    .into_iter()
                    .filter(|(key, _)| keys.contains(key))
                    .map(|(key, role)| json!({"key":key,"role":role}))
                    .collect::<Vec<_>>();
                Ok(Some(json!({"count":peers.len(),"peers":peers})))
            }
            "/api/chain/spec" | "/api/fees" => {
                let block = self.block(tip)?;
                let spec = block.spec.0.resolve(&source, None)?;
                let spec = spec.node.as_ref().unwrap();
                if route.ends_with("spec") {
                    return Ok(Some(
                        json!({"targetBlockTime":spec.target_block_time.to_string(),"initialReward":spec.initial_reward.to_string(),"halvingInterval":spec.halving_interval.to_string(),"maxBlockSize":spec.max_block_size,"maxNumberOfTransactionsPerBlock":spec.max_number_of_transactions_per_block.to_string(),"premine":spec.premine.to_string(),"halfLife":spec.half_life.to_string()}),
                    ));
                }
                let weight = crate::core::mempool::FeeWeight::new(spec);
                let blocks=mining.map(|m|m.mempool().projected_blocks(12,spec)).unwrap_or_default().into_iter().map(|p|json!({"transactionCount":p.transaction_count,"bytes":p.bytes,"lowestFee":num_bigint::BigUint::parse_bytes(p.lowest_fee.to_hex().as_bytes(),16).unwrap().min(num_bigint::BigUint::from(u64::MAX)).to_string(),"lowestFeeBytes":p.lowest_fee_bytes,"lowestFeeWeight":p.lowest_fee_weight,"full":p.full})).collect::<Vec<_>>();
                Ok(Some(
                    json!({"minRelayFee":self.mining.state().mempool().limits().min_relay_fee.to_string(),"blocks":blocks,"weight":{"blockRoom":weight.block_room,"growthRoom":weight.growth_room,"maxTransactions":weight.max_transactions,"transactionAllowance":weight.transaction_allowance}}),
                ))
            }
            "/api/blocks" => {
                let n = limit(q, 10, 25)?;
                let height = tree.height(tip).unwrap_or(0);
                let before = q
                    .get("before")
                    .map(|s| s.parse::<u64>())
                    .transpose()?
                    .unwrap_or(height.saturating_add(1))
                    .min(height.saturating_add(1));
                let mut at = Some(tip);
                let mut blocks = vec![];
                let mut last = before;
                while let Some(cid) = at {
                    let h = tree.height(cid).unwrap();
                    if h < before
                        && blocks.len() < n
                        && let Some(mut value) = self.level_block_view(tree, cid, false)?
                    {
                        value.as_object_mut().unwrap().remove("rewardCredited");
                        blocks.push(value);
                        last = h;
                    }
                    if blocks.len() >= n {
                        break;
                    }
                    at = tree.parent(cid);
                }
                let mut value = json!({"blocks":blocks});
                if last > 0 {
                    value["nextBefore"] = json!(last.to_string());
                }
                Ok(Some(value))
            }
            "/api/chain/genesis" => Ok(Some(json!({"genesisHash":genesis}))),
            route
                if route.starts_with("/api/transaction/")
                    || route.starts_with("/transactions/") =>
            {
                let cid = route.rsplit('/').next().unwrap();
                let tx = Header::<Transaction>::from_cid(cid)?.resolve(&source, None)?;
                let body = tx.node.as_ref().unwrap().body.resolve(&source, None)?;
                if body.node.as_ref().unwrap().chain_path != path {
                    return Ok(None);
                }
                self.read_value(route, q)
            }
            "/api/block/latest" => self.level_block_view(tree, tip, false),
            route if route.starts_with("/api/block/") => {
                let id = route.trim_start_matches("/api/block/");
                if let Some((cid, kind)) = id.split_once('/') {
                    if !tree.contains(cid) {
                        return Ok(None);
                    }
                    return self.block_members(&self.block(cid)?, kind, q);
                }
                let mut cid = id;
                if let Ok(height) = id.parse::<u64>() {
                    cid = tip;
                    while tree.height(cid).is_some_and(|h| h > height) {
                        let Some(parent) = tree.parent(cid) else {
                            return Ok(None);
                        };
                        cid = parent;
                    }
                    if tree.height(cid) != Some(height) {
                        return Ok(None);
                    }
                }
                self.level_block_view(tree, cid, true)
            }
            route
                if route.starts_with("/accounts/") || route.starts_with("/api/state/account/") =>
            {
                let owner = route
                    .strip_prefix("/accounts/")
                    .or_else(|| route.strip_prefix("/api/state/account/"))
                    .unwrap();
                let cid = q.get("block").map(String::as_str).unwrap_or(tip);
                if !tree.is_executed(cid) {
                    return Ok(None);
                }
                let block = Header::<Block>::from_cid(cid)?.resolve(&source, None)?;
                let state = block
                    .node
                    .as_ref()
                    .unwrap()
                    .post_state
                    .0
                    .resolve(&source, None)?;
                let accounts = state
                    .node
                    .as_ref()
                    .unwrap()
                    .account_state
                    .0
                    .resolve(&source, None)?;
                let nonce_key = format!("{}{owner}", lattice_validation::NONCE_PREFIX);
                let accounts = accounts
                    .node
                    .as_ref()
                    .unwrap()
                    .resolve_key(owner, &source)?
                    .resolve_key(&nonce_key, &source)?;
                let balance = accounts.get(owner)?.copied().unwrap_or(0);
                let nonce =
                    lattice_primitives::next_expected_nonce(accounts.get(&nonce_key)?.copied())?;
                let mut result =
                    json!({"owner":owner,"balance":balance.to_string(),"nonce":nonce.to_string()});
                if route.starts_with("/accounts/") {
                    result["block"] = json!(cid)
                }
                Ok(Some(result))
            }
            _ => Ok(None),
        }
    }
    fn transaction_inclusion(
        &self,
        cid: &str,
        body: &lattice_primitives::TransactionBody,
    ) -> anyhow::Result<Option<(u64, String, i64)>> {
        let (graph, tip_cid) = if body.chain_path == ["Nexus"] {
            (self.chain.tree(), self.mining.state().tip_cid())
        } else {
            let Some(core) = self.levels.get(&body.chain_path) else {
                return Ok(None);
            };
            let Some(mining) = self.child_mining.get(&body.chain_path) else {
                return Ok(None);
            };
            (core.tree(), mining.core.state().tip_cid())
        };
        let canonical_at = |height: u64| -> Option<String> {
            let mut at = tip_cid;
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
        let Some(signer) = body.signers.first() else {
            return Ok(None);
        };
        let source = ReadSource(self.storage.as_ref());
        let next_nonce = |block: &Block| -> anyhow::Result<u64> {
            let state = block.post_state.0.resolve(&source, None)?;
            let accounts = state
                .node
                .as_ref()
                .unwrap()
                .account_state
                .0
                .resolve(&source, None)?;
            let key = format!("{}{signer}", lattice_validation::NONCE_PREFIX);
            let tree = accounts.node.as_ref().unwrap().resolve_key(&key, &source)?;
            Ok(lattice_primitives::next_expected_nonce(
                tree.get(&key)?.copied(),
            )?)
        };
        let tip = self.block(tip_cid)?;
        if next_nonce(&tip)? <= body.nonce {
            return Ok(None);
        }
        let mut low = 0;
        let mut high = tip.height;
        while low < high {
            let middle = low + (high - low) / 2;
            let Some(hash) = canonical_at(middle) else {
                return Ok(None);
            };
            if next_nonce(&self.block(&hash)?)? > body.nonce {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        let Some(hash) = canonical_at(high) else {
            return Ok(None);
        };
        let block = self.block(&hash)?;
        let transactions = block.transactions.resolve(&source, None)?;
        let tree = transactions.node.as_ref().unwrap();
        let entries = tree
            .resolve_range_with_keys(None, tree.count, &source, &())?
            .sorted_entries(None, tree.count)?;
        Ok(entries
            .iter()
            .any(|(_, tx)| tx.0.raw_cid == cid)
            .then_some((high, hash, block.timestamp)))
    }
    pub(super) fn canonical_at(&self, height: u64) -> Option<String> {
        let mut at = self.chain.tree().executed_prefix()?;
        loop {
            let h = self.chain.tree().height(at)?;
            if h == height {
                return Some(at.into());
            }
            if h < height {
                return None;
            }
            at = self.chain.tree().parent(at)?;
        }
    }
    fn block_view(&self, cid: &str, full: bool) -> anyhow::Result<Option<Value>> {
        self.level_block_view(self.chain.tree(), cid, full)
    }
    fn level_block_view(
        &self,
        tree: &lattice_block_tree::ChainTree,
        cid: &str,
        full: bool,
    ) -> anyhow::Result<Option<Value>> {
        if !tree.contains(cid) {
            return Ok(None);
        }
        let source = ReadSource(self.storage.as_ref());
        let header = Header::<Block>::from_cid(cid)?.resolve(&source, None)?;
        let block = header.node.as_ref().unwrap();
        let count = block
            .transactions
            .resolve(&source, None)
            .ok()
            .and_then(|h| h.node.as_ref().map(|d| d.count))
            .unwrap_or(0);
        let mut value = json!({"height":block.height.to_string(),"hash":cid,"transactionCount":count,"timestamp":block.timestamp.to_string()});
        optional(
            &mut value,
            "previousBlock",
            block.parent.as_ref().map(|h| json!(h.0.raw_cid)),
        );
        optional(
            &mut value,
            "rewardRecipient",
            block.reward_recipient.as_ref().map(|s| json!(s)),
        );
        let reward = if block.reward_recipient.is_none() {
            Some("0".into())
        } else {
            (self.transactions(cid).ok()).and_then(|txs| {
                let spec = block.spec.0.resolve(&source, None).ok()?;
                let bodies = txs
                    .into_iter()
                    .map(|tx| tx.body.node.as_deref().cloned())
                    .collect::<Option<Vec<_>>>()?;
                lattice_validation::coinbase_amount(
                    &lattice_validation::TransitionActions::from_bodies(&bodies),
                    spec.node.as_ref()?.reward_at_block(block.height),
                )
                .ok()
                .map(|v| v.to_string())
            })
        };
        optional(&mut value, "rewardCredited", reward.map(|s| json!(s)));
        if full {
            let count = block
                .children
                .resolve(&source, None)
                .ok()
                .and_then(|h| h.node.as_ref().map(|d| d.len()))
                .unwrap_or(0);
            for(k,v)in json!({"childBlockCount":count,"nonce":block.nonce.to_string(),"version":block.version,"target":block.target,"nextTarget":block.next_target,"transactionsCID":block.transactions.raw_cid,"postStateCID":block.post_state.0.raw_cid,"chain":tree.context().unwrap().path()}).as_object().unwrap(){value[k]=v.clone();}
        }
        Ok(Some(value))
    }
}
fn limit(q: &BTreeMap<String, String>, default: usize, cap: usize) -> anyhow::Result<usize> {
    Ok(q.get("limit")
        .map(|s| s.parse::<usize>())
        .transpose()?
        .unwrap_or(default)
        .min(cap))
}
