pub use super::super::network::ReadEndpointConfig;
use super::*;
use crate::core::read_endpoints::{DeclaredReadEndpoint, rendezvous, valid_path};
impl Host {
    pub(super) fn read_endpoint_keys(&self) -> Vec<String> {
        if self.read_endpoint_config.public_read_url.is_none() {
            return vec![];
        }
        self.levels
            .ordered()
            .iter()
            .map(|p| rendezvous(&self.genesis, p))
            .collect()
    }
    pub(super) fn endpoint_path(&self, route: &str) -> Result<Option<Vec<String>>, anyhow::Error> {
        let Some((base, query)) = route.split_once('?') else {
            if route == "/api/chain/endpoints" {
                anyhow::bail!("invalidChainPath");
            }
            return Ok(None);
        };
        if base != "/api/chain/endpoints" {
            return Ok(None);
        }
        let raw = form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == "chainPath")
            .map(|(_, v)| v.into_owned())
            .ok_or_else(|| anyhow::anyhow!("invalidChainPath"))?;
        let path = raw
            .split('/')
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        anyhow::ensure!(path.len() > 1 && valid_path(&path), "invalidChainPath");
        Ok(Some(path))
    }
    pub(super) fn read_endpoint_result(
        &self,
        path: &[String],
        remote: Vec<DeclaredReadEndpoint>,
    ) -> (u16, Value) {
        let parent_path = &path[..path.len() - 1];
        let tree = if parent_path == ["Nexus"] {
            Some(self.chain.tree())
        } else {
            self.levels.get(parent_path).map(|c| c.tree())
        };
        let Some(tree) = tree else {
            return (404, json!({"error":{"message":"unknownChain"}}));
        };
        let hosts = self.levels.get(path).is_some();
        let mut declared = if hosts {
            self.read_endpoint_config
                .public_read_url
                .as_ref()
                .map(|url| {
                    vec![DeclaredReadEndpoint {
                        url: url.clone(),
                        accepts_submit: self.read_endpoint_config.public_submit,
                    }]
                })
                .unwrap_or_default()
        } else {
            vec![]
        };
        for endpoint in remote {
            if !declared.iter().any(|d| d.url == endpoint.url) {
                declared.push(endpoint);
            }
        }
        let mut tip = tree.heaviest_executed_tip(&BTreeSet::new());
        let mut committed = None;
        for _ in 0..64 {
            if tip.is_empty() {
                break;
            }
            if let Ok(block) = self.block(&tip)
                && let Ok(children) = block.children.resolve(self.storage.as_ref(), None)
                && let Some(child) = children
                    .node
                    .as_ref()
                    .and_then(|d| d.get(path.last().unwrap()))
            {
                committed = Some(child.0.raw_cid.clone());
                break;
            }
            let Some(parent) = tree.parent(&tip) else {
                break;
            };
            tip = parent.into();
        }
        if !hosts && committed.is_none() && declared.is_empty() {
            return (404, json!({"error":{"message":"unknownChain"}}));
        }
        let mut result = json!({"chainPath":path,"endpoints":declared.iter().map(|d|&d.url).collect::<Vec<_>>(),"submitEndpoints":declared.iter().filter(|d|d.accepts_submit).map(|d|&d.url).collect::<Vec<_>>()});
        if let Some(cid) = committed {
            result["committedBlock"] = json!(cid);
        }
        (200, result)
    }
}
