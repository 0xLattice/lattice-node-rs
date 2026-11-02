#!/usr/bin/env python3
"""Compare actual pinned Swift and Rust metric renderers; no network required."""
import json
import pathlib
import subprocess
import sys
import tempfile

rust = pathlib.Path(__file__).resolve().parents[1]
swift = pathlib.Path(sys.argv[1]).resolve()
assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=swift, text=True).strip() == "3df9e0a786c7b103a3da88d1c3881efc81706255"
relative = "Sources/LatticeNode/Observability/NodeMetrics.swift"
assert not subprocess.check_output(["git", "diff", "HEAD", "--", relative], cwd=swift)
source = (swift / relative).read_text()
source += r'''
let samples: [NodeMetricsSample] = [
    NodeMetricsSample(chainPath: ["Nexus"], validatedTipHeight: 12, weighedTipHeight: 15, overlayPeers: 3, mempoolTransactions: 7, processStartTime: Date(timeIntervalSince1970: 123.5)),
    NodeMetricsSample(chainPath: ["Nexus"], validatedTipHeight: nil, weighedTipHeight: nil, overlayPeers: 0, mempoolTransactions: 0, processStartTime: Date(timeIntervalSince1970: 456.25)),
    NodeMetricsSample(chainPath: ["Nexus", "quote\"slash\\line\r\n"], validatedTipHeight: UInt64.max, weighedTipHeight: nil, overlayPeers: 2, mempoolTransactions: 4, processStartTime: Date(timeIntervalSince1970: 789.125)),
]
print(String(data: try JSONSerialization.data(withJSONObject: samples.map(renderNodeMetrics)), encoding: .utf8)!)
'''
with tempfile.TemporaryDirectory(prefix="lattice-metrics-oracle-") as directory:
    path = pathlib.Path(directory) / "main.swift"
    path.write_text(source)
    expected = json.loads(subprocess.check_output(["swift", str(path)], text=True))
actual = json.loads(subprocess.check_output(["cargo", "run", "--locked", "--quiet", "--example", "metrics_samples"], cwd=rust, text=True))
assert actual == expected, "Swift/Rust metrics exposition differs"
print("PASS: three exact Swift/Rust metric samples (active, awaiting genesis, escaped labels/u64 max)")
