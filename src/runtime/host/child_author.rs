//! Operator child-author requests. Built content carries no admission authority.
use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use lattice_cashew::{FlatDictionary, cbor};
use lattice_primitives::{BlockHeader, ChainSpec, LatticeState, child_directory};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Push {
    chain_path: Vec<String>,
    block: String,
    children: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    spec: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Build {
    chain_path: Vec<String>,
    recipient: Option<String>,
}
fn path_ok(path: &[String]) -> bool {
    path.len() > 1 && path[0] == "Nexus" && path.iter().skip(1).all(|p| child_directory(p))
}
fn decode<T: DeserializeOwned + Serialize>(value: &str) -> anyhow::Result<T> {
    let bytes = STANDARD
        .decode(value)
        .map_err(|_| anyhow::anyhow!("malformedBlock"))?;
    let node = cbor::decode(&bytes).map_err(|_| anyhow::anyhow!("malformedBlock"))?;
    anyhow::ensure!(cbor::encode(&node)? == bytes, "malformedBlock");
    Ok(node)
}
impl Host {
    pub(super) fn child_author(&mut self, route: &str, body: &Value) -> Option<(u16, Value)> {
        let result = if route == "/child-blocks/build" {
            Some(self.build_child(body))
        } else if route == "/child-blocks" {
            Some(self.push_child(body))
        } else if let Some(query) = route.strip_prefix("/child-blocks/commitment?") {
            let path = form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "chainPath")
                .map(|(_, v)| {
                    v.split('/')
                        .filter(|p| !p.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            Some(self.read_child_commitment(&path))
        } else if route == "/child-blocks/commitment" {
            Some(Err(anyhow::anyhow!("invalidChainPath")))
        } else {
            None
        };
        result.map(|r| match r {
            Ok(value) => (200, value),
            Err(e) => error(e),
        })
    }
    fn commitment_value(&self, path: &[String], state: &str) -> anyhow::Result<Option<String>> {
        let state =
            Header::<LatticeState>::from_cid(state)?.resolve(self.storage.as_ref(), None)?;
        let general = state
            .node
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missingMaterializedVolume"))?
            .general_state
            .0
            .resolve(self.storage.as_ref(), None)?;
        let key = format!("child/{}", path.last().unwrap());
        let dictionary = general
            .node
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missingMaterializedVolume"))?
            .resolve_key(&key, self.storage.as_ref())?;
        Ok(dictionary.get(&key)?.cloned())
    }
    fn read_child_commitment(&self, path: &[String]) -> anyhow::Result<Value> {
        anyhow::ensure!(path_ok(path), "invalidChainPath");
        anyhow::ensure!(self.levels.get(path).is_some(), "unknownChain");
        let tree = self.child_parent(path)?;
        let tip = tree.heaviest_executed_tip(&BTreeSet::new());
        anyhow::ensure!(
            tree.has_executed_ancestry(&tip),
            "missingMaterializedVolume"
        );
        let state = self.block(&tip)?.post_state.0.raw_cid;
        let value = self.commitment_value(path, &state)?;
        let mut result = json!({"parentState":state});
        if let Some(value) = value {
            result["value"] = json!(value)
        };
        Ok(result)
    }
    fn build_child(&mut self, body: &Value) -> anyhow::Result<Value> {
        let request: Build = serde_json::from_value(body.clone())?;
        let path = request.chain_path;
        anyhow::ensure!(path_ok(&path), "invalidChainPath");
        if let Some(recipient) = &request.recipient {
            anyhow::ensure!(
                lattice_primitives::standard_cid(recipient),
                "invalidRecipientPlan"
            );
        }
        anyhow::ensure!(self.levels.get(&path).is_some(), "unknownChain");
        let state = self.mining.state();
        let tip = state.tip_cid().to_owned();
        let mut plan = TemplateRequest::default();
        if let Some(recipient) = request.recipient {
            plan.child_recipients.insert(path.clone(), recipient);
        }
        let job = TemplateJob {
            id: 0,
            tip_cid: tip.clone(),
            tip_epoch: state.tip_epoch(),
            pool_version: state.mempool().version(),
            transactions: state
                .mempool()
                .items()
                .into_iter()
                .map(|i| i.transaction)
                .collect(),
            request: plan,
        };
        let anchor = self.chain.tree().difficulty_anchor(&tip).map(|a| {
            lattice_validation::DifficultyAnchor {
                block_height: a.block_height,
                timestamp: a.timestamp,
                target: UInt256(a.target),
            }
        });
        let contexts = self.child_templates(&tip)?;
        let output = super::super::merged_template::build_merged_template(
            &job,
            self.storage.as_ref(),
            now(),
            anchor,
            &contexts,
        )?;
        self.child_policy_refused(&output.refused)?;
        self.event(MiningEvent::PolicyRefused {
            tip_cid: tip,
            cids: output
                .refused
                .get(&vec!["Nexus".into()])
                .cloned()
                .unwrap_or_default(),
        })?;
        let mut block = output.build.block;
        for directory in path.iter().skip(1) {
            block = block
                .children
                .node
                .as_ref()
                .and_then(|d| d.get(directory))
                .and_then(|h| h.0.node.as_deref())
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("noChildCandidate"))?;
        }
        let cid = content_id(&block)?;
        let parent_state = block.parent_state.0.raw_cid.clone();
        let volumes =
            lattice_cashew::volume::serialize_recursive(&Volume::new(block.clone())?, &())?;
        let scope = format!("author-{cid}");
        self.storage.cache_volumes(&scope, &volumes)?;
        // Retain the same content boundaries as a mined block; do not publish facts.
        let retained = (|| -> anyhow::Result<()> {
            let mut pending = vec![(path.clone(), block.clone())];
            while let Some((path, block)) = pending.pop() {
                let cid = content_id(&block)?;
                let index = block.transactions.resolve(self.storage.as_ref(), None)?;
                let index = index
                    .node
                    .as_ref()
                    .unwrap()
                    .resolve_all(self.storage.as_ref())?;
                let mut body = BTreeSet::from([cid.clone()]);
                body.extend(index.entries()?.into_values().map(|t| t.0.raw_cid.clone()));
                let roots =
                    lattice_cashew::volume::serialize_recursive(&Volume::new(block.clone())?, &())?
                        .into_iter()
                        .map(|v| v.root)
                        .collect();
                self.storage.retain_stored_content(
                    self.storage.retention_policy().state,
                    &roots,
                    &body,
                    &cid,
                    &path,
                )?;
                if let Some(children) = block.children.node.as_ref() {
                    for (d, h) in children.iter() {
                        if let Some(b) = h.0.node.as_deref() {
                            let mut p = path.clone();
                            p.push(d.clone());
                            pending.push((p, b.clone()));
                        }
                    }
                }
            }
            Ok(())
        })();
        if retained.is_ok() {
            self.storage.release_cached_volumes(&scope)?;
        } else {
            self.transient.push(scope);
        }
        retained?;
        let children = block.children.resolve(self.storage.as_ref(), None)?;
        let spec = if block.parent.is_none() {
            Some(
                STANDARD.encode(cbor::encode(
                    block
                        .spec
                        .0
                        .resolve(self.storage.as_ref(), None)?
                        .node
                        .as_ref()
                        .unwrap()
                        .as_ref(),
                )?),
            )
        } else {
            None
        };
        let submit = Push {
            chain_path: path.clone(),
            block: STANDARD.encode(cbor::encode(&block)?),
            children: STANDARD.encode(cbor::encode(children.node.as_ref().unwrap().as_ref())?),
            spec,
        };
        let previous = self.commitment_value(&path, &parent_state)?;
        let mut result = json!({"blockCID":cid,"parentState":parent_state,"submit":submit});
        if let Some(value) = previous {
            result["previous"] = json!(value)
        };
        Ok(result)
    }
    fn push_child(&mut self, body: &Value) -> anyhow::Result<Value> {
        let push: Push = serde_json::from_value(body.clone())?;
        let path = push.chain_path;
        anyhow::ensure!(path_ok(&path), "invalidChainPath");
        let block: Block = decode(&push.block)?;
        let children: FlatDictionary<BlockHeader> = decode(&push.children)?;
        let spec: Option<ChainSpec> = push.spec.as_deref().map(decode).transpose()?;
        anyhow::ensure!(self.levels.get(&path).is_some(), "unknownChain");
        let cid = content_id(&block)?;
        let parent = self.child_parent(&path)?;
        self.levels.get_mut(&path).unwrap().queue_header(
            HeaderEntry {
                block,
                children: Some(children),
                proofs: vec![],
                spec,
            },
            None,
            Some(&parent),
            now(),
        )?;
        self.child_proofs(&path)?;
        let status = self.levels.get(&path).unwrap().header_status(&cid);
        Ok(json!({"blockCID":cid,"status":status}))
    }
}
