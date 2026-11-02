use super::*;
pub(super) struct ChildMining {
    pub core: DurableMiningCore,
    parent_tip: String,
}
impl Host {
    pub(super) fn refresh_child_mining(&mut self) -> anyhow::Result<()> {
        for path in self.levels.ordered() {
            let tree = self.levels.get(&path).unwrap().tree();
            let tip = tree.heaviest_executed_tip(&BTreeSet::new());
            if !tree.has_executed_ancestry(&tip) {
                continue;
            }
            let parent = self.child_parent(&path)?;
            let parent_tip = parent.heaviest_executed_tip(&BTreeSet::new());
            if let Some(mining) = self.child_mining.get(&path) {
                if mining.core.state().tip_cid() == tip && mining.parent_tip == parent_tip {
                    continue;
                }
                let old = mining.core.state().tip_cid().to_owned();
                let mut ancestry = BTreeSet::new();
                let mut at = Some(old.as_str());
                while let Some(cid) = at {
                    ancestry.insert(cid.to_owned());
                    at = tree.parent(cid);
                }
                let mut entered = vec![];
                let mut at = tip.clone();
                while !ancestry.contains(&at) {
                    entered.push(at.clone());
                    let Some(parent) = tree.parent(&at) else {
                        break;
                    };
                    at = parent.into();
                }
                let common = at;
                let mut left = vec![];
                let mut at = old;
                while at != common {
                    left.push(at.clone());
                    let Some(parent) = tree.parent(&at) else {
                        break;
                    };
                    at = parent.into();
                }
                let mut confirmed = BTreeSet::new();
                for cid in entered {
                    for tx in self.transactions(&cid)? {
                        confirmed.insert(content_id(&tx)?);
                    }
                }
                let mut returned = vec![];
                for cid in left.iter().rev() {
                    returned.extend(
                        self.transactions(cid)?
                            .into_iter()
                            .filter(|tx| content_id(tx).is_ok_and(|cid| !confirmed.contains(&cid))),
                    );
                }
                let spec = self
                    .block(&tip)?
                    .spec
                    .0
                    .resolve(self.storage.as_ref(), None)?
                    .node
                    .as_deref()
                    .unwrap()
                    .clone();
                let mining = self.child_mining.get_mut(&path).unwrap();
                let effects = mining.core.move_tip_with_spec(
                    TipMove {
                        tip_cid: tip,
                        confirmed,
                        returned,
                        left,
                    },
                    spec,
                    now(),
                )?;
                mining.parent_tip = parent_tip;
                self.enqueue_child_mining(&path, effects);
            } else {
                let spec = self
                    .block(&tip)?
                    .spec
                    .0
                    .resolve(self.storage.as_ref(), None)?
                    .node
                    .as_deref()
                    .unwrap()
                    .clone();
                let state = MiningState::new(
                    tip,
                    Some(spec),
                    MiningConfig {
                        mempool: crate::core::mempool::MempoolLimits {
                            min_relay_fee: self.mining.state().mempool().limits().min_relay_fee,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                );
                let (core, effects) =
                    DurableMiningCore::restore(state, self.storage.clone(), &path, now())?;
                self.child_mining
                    .insert(path.clone(), ChildMining { core, parent_tip });
                self.enqueue_child_mining(&path, effects);
            }
        }
        Ok(())
    }
    fn enqueue_child_mining(&mut self, path: &[String], effects: Vec<MiningEffect>) {
        self.queue.extend(
            effects
                .into_iter()
                .map(|e| Work::ChildMining(path.to_vec(), Box::new(e))),
        );
    }
    pub(super) fn receive_transaction(
        &mut self,
        tx: Transaction,
        origin: TransactionOrigin,
    ) -> anyhow::Result<()> {
        let path = tx
            .body
            .node
            .as_ref()
            .map(|b| b.chain_path.clone())
            .unwrap_or_default();
        if path == ["Nexus"] {
            return self.event(MiningEvent::TransactionReceived(tx, origin));
        }
        if let Some(mining) = self.child_mining.get_mut(&path) {
            let effects = mining
                .core
                .step(MiningEvent::TransactionReceived(tx, origin), now())?;
            self.enqueue_child_mining(&path, effects);
        } else if let TransactionOrigin::Submitted(id) | TransactionOrigin::Local(id) = origin {
            self.reply(id, error("chain is not ready for transaction submission"));
        }
        Ok(())
    }
    pub(super) async fn child_mining_effect(
        &mut self,
        path: Vec<String>,
        effect: MiningEffect,
    ) -> anyhow::Result<()> {
        if !self.child_mining.contains_key(&path) {
            return Ok(());
        }
        match effect {
            MiningEffect::Preflight(job) => {
                let parent = self.child_parent(&path)?;
                let tree = self.levels.get(&path).unwrap().tree();
                let mining = self.child_mining.get_mut(&path).unwrap();
                if let Some(done) =
                    mining
                        .core
                        .execute_child_preflight(&job, tree, &parent, now())?
                {
                    let effects = mining.core.complete_child_preflight(done, &parent, now())?;
                    self.enqueue_child_mining(&path, effects);
                }
            }
            MiningEffect::TransactionAdmitted {
                reply_id,
                cid,
                count,
                bytes,
            } => self.reply(
                reply_id,
                (
                    200,
                    json!({"transactionCID":cid,"mempoolCount":count,"mempoolBytes":bytes}),
                ),
            ),
            MiningEffect::TransactionRefused(id, e) => self.reply(id, error_fn(e)),
            MiningEffect::AnnounceTransaction(cid) => {
                let bytes = serde_json::to_vec(&json!({"volumeRootCID":cid}))?;
                for peer in self.network.peers_hosting(&path) {
                    self.network
                        .send(
                            peer,
                            super::super::handshake::TRANSACTION_AVAILABLE.into(),
                            bytes.clone(),
                            0,
                        )
                        .await?;
                }
            }
            MiningEffect::ReturnTransactions { left, carried } => {
                for cid in left {
                    for tx in self.transactions(&cid)? {
                        if !carried.contains(&content_id(&tx)?) {
                            self.receive_transaction(tx, TransactionOrigin::Returned)?;
                        }
                    }
                }
            }
            // Child work is currently requested through the merged root template.
            MiningEffect::TemplateRefused(id, e) | MiningEffect::WorkRefused(id, e) => {
                self.reply(id, error_fn(e))
            }
            MiningEffect::BuildTemplate(_)
            | MiningEffect::TemplateIssued(_, _)
            | MiningEffect::Mined(_, _)
            | MiningEffect::PoolChanged(_) => anyhow::bail!("unexpected child mining effect"),
        }
        Ok(())
    }
}

impl Host {
    pub(super) fn child_policy_refused(
        &mut self,
        refused: &BTreeMap<Vec<String>, Vec<String>>,
    ) -> anyhow::Result<()> {
        for (path, cids) in refused {
            if let Some(mining) = self.child_mining.get_mut(path) {
                let tip = mining.core.state().tip_cid().to_owned();
                let effects = mining.core.step(
                    MiningEvent::PolicyRefused {
                        tip_cid: tip,
                        cids: cids.clone(),
                    },
                    now(),
                )?;
                self.enqueue_child_mining(path, effects);
            }
        }
        Ok(())
    }
}
