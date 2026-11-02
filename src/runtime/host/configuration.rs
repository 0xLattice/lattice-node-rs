//! Operator flag-shaped hosted configuration. Parse all inputs before mutation.
use super::*;
use std::io::Read;
#[derive(Clone, Default)]
pub(super) struct ChildSettings {
    pub spec: Option<lattice_primitives::ChainSpec>,
    pub premine_recipient: Option<String>,
    pub genesis_transactions: Vec<Transaction>,
    pub peers: Vec<ivy_core::PeerEndpoint>,
}
pub(super) struct HostedConfiguration {
    pub children: BTreeMap<Vec<String>, ChildSettings>,
    pub modules: Vec<Vec<u8>>,
}
fn entries(body: &Value, name: &str, required: bool) -> anyhow::Result<Vec<String>> {
    let Some(value) = body.get(name).filter(|v| !v.is_null()) else {
        if required {
            anyhow::bail!("missing {name}")
        }
        return Ok(vec![]);
    };
    let values = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("invalid {name}"))?;
    if values.len() > 256 {
        anyhow::bail!("too many {name} entries")
    }
    values
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("invalid {name} entry"))
        })
        .collect()
}
fn path(value: &str) -> anyhow::Result<Vec<String>> {
    let path = value
        .split('/')
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let context = lattice_primitives::ChainRuntimeContext::new(path.clone(), None)?;
    if context.is_root() {
        anyhow::bail!("hostChain must name a child level")
    }
    Ok(path)
}
fn file(path: &str) -> anyhow::Result<Vec<u8>> {
    let mut bytes = vec![];
    std::fs::File::open(path)?
        .take(4 * 1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4 * 1024 * 1024 {
        anyhow::bail!("hosted configuration file exceeds 4 MiB")
    }
    Ok(bytes)
}
fn assignment(entry: &str) -> anyhow::Result<(Vec<String>, &str)> {
    let (p, value) = entry
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("expected chain-path=value"))?;
    if value.is_empty() {
        anyhow::bail!("empty assignment value")
    }
    Ok((path(p)?, value))
}
pub(super) fn parse(body: &Value) -> anyhow::Result<HostedConfiguration> {
    let mut children: BTreeMap<Vec<String>, ChildSettings> = BTreeMap::new();
    for entry in entries(body, "hostChain", true)? {
        let (p, spec) = entry
            .split_once('=')
            .map_or((entry.as_str(), None), |(p, s)| (p, Some(s)));
        let settings = children.entry(path(p)?).or_default();
        if settings.spec.is_none()
            && let Some(spec) = spec
        {
            settings.spec = Some(serde_json::from_slice(&file(spec)?)?);
        }
    }
    for path in children.keys() {
        if path.len() > 2 && !children.contains_key(&path[..path.len() - 1]) {
            anyhow::bail!("hosted parent must also be configured")
        }
    }
    for entry in entries(body, "premineTo", false)? {
        let (p, to) = assignment(&entry)?;
        let settings = children
            .get_mut(&p)
            .filter(|s| s.spec.is_some())
            .ok_or_else(|| anyhow::anyhow!("premineTo requires a hosted spec"))?;
        settings.premine_recipient = Some(to.into());
    }
    let mut genesis_paths = BTreeSet::new();
    for entry in entries(body, "genesisTransactions", false)? {
        let (p, name) = assignment(&entry)?;
        if !genesis_paths.insert(p.clone()) {
            anyhow::bail!("duplicate genesisTransactions path")
        }
        let settings = children
            .get_mut(&p)
            .filter(|s| s.spec.is_some())
            .ok_or_else(|| anyhow::anyhow!("genesisTransactions requires a hosted spec"))?;
        let values: Vec<Value> = serde_json::from_slice(&file(name)?)?;
        settings.genesis_transactions = values
            .into_iter()
            .map(|v| {
                v.get("transaction")
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("missing genesis transaction"))
                    .and_then(|v| super::super::transaction::decode(v).map_err(anyhow::Error::msg))
            })
            .collect::<anyhow::Result<_>>()?;
    }
    for entry in entries(body, "chainPeer", false)? {
        let (p, address) = assignment(&entry)?;
        let settings = children
            .get_mut(&p)
            .ok_or_else(|| anyhow::anyhow!("chainPeer names an unhosted chain"))?;
        let (key, address) = address
            .split_once('@')
            .ok_or_else(|| anyhow::anyhow!("expected public-key@host:port"))?;
        let (host, port) = address
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("expected host:port"))?;
        let key_bytes = hex::decode(key)?;
        if key_bytes.len() != 32 || host.is_empty() {
            anyhow::bail!("invalid peer endpoint")
        }
        let port = port.parse::<u16>()?;
        if port == 0 {
            anyhow::bail!("invalid peer port")
        }
        settings.peers.push(ivy_core::PeerEndpoint {
            public_key: hex::encode(key_bytes),
            host: host.trim_matches(['[', ']']).into(),
            port,
        });
    }
    let modules = entries(body, "policyModule", false)?
        .iter()
        .map(|p| file(p))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (path, settings) in &children {
        if let Some(spec) = &settings.spec {
            super::super::hosted_config::check_premine(
                spec,
                settings.premine_recipient.as_deref(),
                &settings.genesis_transactions,
                path,
                &modules,
                now(),
            )?;
        }
    }
    Ok(HostedConfiguration { children, modules })
}
