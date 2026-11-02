#!/usr/bin/env python3
"""Pinned original Swift window selector and boundary decision, extracted verbatim."""
import hashlib, json, pathlib, subprocess, tempfile
ROOT=pathlib.Path(__file__).resolve().parents[1]
SWIFT=ROOT.parent/'lattice-node'
PIN='3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=SWIFT,text=True).strip()==PIN
assert not subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],cwd=SWIFT)
core=SWIFT/'Sources/LatticeNodeCore/Chain/ChainCore.swift'
selection=SWIFT/'Sources/LatticeNodeCore/Chain/ChainSelection.swift'
text=core.read_text(); start=text.index('public struct FinalityWindows:'); end=text.index('\npublic struct ChainCoreConfig:',start)
windows=text[start:end]
text=selection.read_text(); start=text.index('    func pastFinality(height: UInt64)'); end=text.index('\n    /// Whether weighed block',start)
decision=text[start:end]
source='''import Foundation
'''+windows+'''
struct Tree { var canonicalTip: String; var tip: UInt64; func getHighestBlockHeight() -> UInt64 { tip } }
struct ChainCoreConfig { var finality: FinalityWindows }
struct ChainCore { var config: ChainCoreConfig; var chainPath: [String]; var tree: Tree }
extension ChainCore {
'''+decision+'''
}
var rows: [[String:Any]] = []
for rawDefault: UInt64? in [nil,0,1,3,UInt64.max] {
 for override: UInt64? in [nil,0,2,UInt64.max] {
  var byChain: [[String]:UInt64?] = [:]
  var rawOverrides: [String:UInt64] = [:]
  if let value = override { byChain[["Nexus","A"]] = .some(value > 0 ? value : nil); rawOverrides["Nexus/A"] = value }
  let defaults = rawDefault.flatMap { $0 > 0 ? $0 : nil }
  let windows = FinalityWindows(all: defaults, byChain: byChain)
  for path in [["Nexus"],["Nexus","A"],["Nexus","A","B"]] {
   for tip: UInt64 in [0,1,4,UInt64.max] {
    for candidate: UInt64 in [0,1,2,4,UInt64.max] {
     let core = ChainCore(config: ChainCoreConfig(finality:windows),chainPath:path,tree:Tree(canonicalTip:"held",tip:tip))
     rows.append(["default":rawDefault.map { $0 as Any } ?? NSNull(),"overrides":rawOverrides,"path":path.joined(separator:"/"),"tip":tip,"candidate":candidate,"window":windows.window(for:path).map { $0 as Any } ?? NSNull(),"declined":core.pastFinality(height:candidate)])
    }
   }
  }
 }
}
let data = try JSONSerialization.data(withJSONObject: rows, options:[.sortedKeys])
FileHandle.standardOutput.write(data)
'''
with tempfile.TemporaryDirectory(prefix='lattice-finality-swift-') as directory:
    base=pathlib.Path(directory); file=base/'main.swift'; file.write_text(source)
    subprocess.run(['swiftc',str(file),'-o',str(base/'oracle')],check=True)
    rows=json.loads(subprocess.check_output([str(base/'oracle')]))
(ROOT/'tests/finality-swift.json').write_text(json.dumps(rows,indent=2)+'\n')
(ROOT/'tests/finality-swift-provenance.json').write_text(json.dumps({'swiftRevision':PIN,'observations':len(rows),'sourceSha256':{str(p.relative_to(SWIFT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [core,selection]},'scope':'Original FinalityWindows and pastFinality(height:) methods; tree height supplied, daemon parser and scheduler excluded'},indent=2)+'\n')
print('Swift finality observations:',len(rows))
