#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
source="$root/../Lattice"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
# Compile UInt256 itself, not a replacement numeric model.
for file in "$root/../UInt256/Sources/"*.swift; do cp "$file" "$tmp/$(basename "$file")"; done
python3 - "$source" "$tmp" <<'PY'
import sys,pathlib
src=pathlib.Path(sys.argv[1]);out=pathlib.Path(sys.argv[2])
def clean(s):return '\n'.join(x for x in s.splitlines() if not x.startswith('import ') or x=='import Foundation').replace('package ','internal ')
out.joinpath('Extensions.swift').write_text(clean(src.joinpath('Sources/LatticePrimitives/UInt256+Extensions.swift').read_text().split('public extension UInt256 {')[0]+'public extension UInt256 {'+src.joinpath('Sources/LatticePrimitives/UInt256+Extensions.swift').read_text().split('public extension UInt256 {')[1]))
s=src.joinpath('Sources/LatticePoW/ProofOfWork.swift').read_text();s=s[s.index('public struct VerifiedWorkContribution:'):s.index('public extension Block {')];out.joinpath('Work.swift').write_text('import Foundation\n'+clean(s)+'\nenum CIDIdentity { static func canonicalString(_ s:String)->String? { s } }\n')
out.joinpath('Diff.swift').write_text(clean(src.joinpath('Sources/LatticePrimitives/State/ReplacedCIDs.swift').read_text().split('public func diffCIDs')[0]))
out.joinpath('Facts.swift').write_text(clean(src.joinpath('Sources/LatticeBlockTree/ChainFacts.swift').read_text()))
PY
swiftc -O "$tmp"/*.swift "$root/tools/GenerateFactVectors.swift" -o "$tmp/generate"
"$tmp/generate" > "$root/tests/fixtures/swift-facts.jsonl"
