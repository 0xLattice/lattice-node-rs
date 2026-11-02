//! Pinned Swift NodeMetrics Prometheus exposition (operator surface only).
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4";
pub struct Sample<'a> {
    pub chain_path: &'a [String],
    pub validated_height: Option<u64>,
    pub weighed_height: Option<u64>,
    pub peers: usize,
    pub mempool_transactions: u64,
    pub process_start_time: f64,
}
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
pub fn render(sample: &Sample<'_>) -> String {
    let chain = format!("chain=\"{}\"", escape(&sample.chain_path.join("/")));
    let mut output = String::new();
    let mut family = |name: &str, help: &str, values: Vec<(String, String)>| {
        output.push_str(&format!("# HELP {name} {help}\n# TYPE {name} gauge\n"));
        for (labels, value) in values {
            output.push_str(&format!("{name}{{{labels}}} {value}\n"));
        }
    };
    family(
        "lattice_chain_tip_height",
        "Main-chain tip height by tier (validated, weighed); absent while awaiting genesis.",
        [
            ("validated", sample.validated_height),
            ("weighed", sample.weighed_height),
        ]
        .into_iter()
        .filter_map(|(tier, value)| {
            value.map(|value| (format!("{chain},tier=\"{tier}\""), value.to_string()))
        })
        .collect(),
    );
    family(
        "lattice_overlay_peers",
        "Authenticated hosted-tree overlay peers; in-process parent/child links are not counted.",
        vec![(chain.clone(), sample.peers.to_string())],
    );
    family(
        "lattice_mempool_transactions",
        "Transactions in the mempool.",
        vec![(chain.clone(), sample.mempool_transactions.to_string())],
    );
    family(
        "process_start_time_seconds",
        "Start time of the process since unix epoch in seconds.",
        vec![(chain, sample.process_start_time.to_string())],
    );
    output
}
