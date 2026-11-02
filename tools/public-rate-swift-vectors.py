#!/usr/bin/env python3
"""Run unchanged pinned Swift limiter and classifier with deterministic clocks."""
import hashlib, json, pathlib, subprocess, tempfile
ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT.parent / 'lattice-node/Sources/LatticeNodeDaemon/PublicReadRateLimit.swift'
PIN = '3df9e0a786c7b103a3da88d1c3881efc81706255'
assert subprocess.check_output(['git','rev-parse','HEAD'], cwd=SOURCE.parents[2], text=True).strip() == PIN
original = SOURCE.read_text()
assert subprocess.check_output(['git','show',PIN+':Sources/LatticeNodeDaemon/PublicReadRateLimit.swift'],cwd=SOURCE.parents[2]) == SOURCE.read_bytes()
# Exact original definitions; external framework types below only supply the
# method constants and configuration error used by these standalone algorithms.
limits = original[original.index('struct PublicReadRateLimits:'):original.index('/// One value, recomputed')]
actor = original[original.index('actor PublicReadRateLimiter {'):original.index("/// The public listener's request context.")]
shim = '''import Foundation
import Dispatch
struct ValidationError: Error { init(_ message: String) {} }
enum HTTPRequest { enum Method: String { case get="GET", head="HEAD", post="POST", options="OPTIONS", delete="DELETE" } }
final class Clock: @unchecked Sendable { var now = 0.0 }
'''
main = '''
@main struct Main {
static func main() async throws {
 let input = try JSONSerialization.jsonObject(with: FileHandle.standardInput.readDataToEndOfFile()) as! [[String: Any]]
 var results = [[String: Any]]()
 for scenario in input {
  let rates = scenario["rates"] as! [Double]
  let limits = try PublicReadRateLimits.validated(generalRate:rates[0], expensiveRate:rates[1], listenerRate:rates[2], submitRate:rates[3])
  let clock = Clock()
  let limiter = PublicReadRateLimiter(limits:limits, clock: { clock.now })
  var observations = [[String: Any]]()
  for event in scenario["events"] as! [[String: Any]] {
   clock.now = event["time"] as! Double
   let route = PublicReadRouteClass(method:HTTPRequest.Method(rawValue:event["method"] as! String)!, path:event["path"] as! String)
   let admitted = await limiter?.admit(client:event["client"] as! String,route:route) ?? true
   let tracked = await limiter?.trackedClientCount ?? 0
   observations.append(["admitted":admitted,"tracked":tracked,"class":String(describing:route)])
  }
  var result = scenario; result["observations"] = observations; results.append(result)
 }
 FileHandle.standardOutput.write(try JSONSerialization.data(withJSONObject:results, options:[.sortedKeys]))
}
}
'''
scenarios=[]
paths=['/health','//health/','/api/block/latest','/api/block/x','//api/block//x/children/','/api/block/x/transactions','/api/transaction/x','/api/chain/endpoints','/api/deposits','/volumes/x','/transactions','/unknown']
for rates in [[25,1,200,10],[0,0,0,0],[.1,.1,.1,.1],[1,0,2,0],[0,1,0,1],[.1,.1,0,0],[2,1,3,2]]:
 events=[]
 for i in range(480):
  events.append(dict(time=0 if i<240 else (i-240)*.125, client=str(i%37), method=['GET','HEAD','POST','OPTIONS','DELETE'][i%5],path=paths[i%len(paths)]))
 # Repeated same-client bursts and backward-clock behavior.
 events += [dict(time=40 if i<100 else 39 if i<150 else 100,client='burst',method='POST' if i%3==0 else 'GET',path='/transactions' if i%3==0 else '/api/deposits') for i in range(200)]
 scenarios.append(dict(rates=rates,events=events))
with tempfile.TemporaryDirectory() as directory:
 source=pathlib.Path(directory)/'Oracle.swift'; binary=pathlib.Path(directory)/'oracle'
 source.write_text(shim+limits+actor+main)
 subprocess.run(['swiftc','-parse-as-library',str(source),'-o',str(binary)],check=True)
 result=subprocess.check_output([str(binary)],input=json.dumps(scenarios).encode())
(ROOT/'tests/public-rate-swift.json').write_bytes(result+b'\n')
(ROOT/'tests/public-rate-swift-provenance.json').write_text(json.dumps(dict(commit=PIN,source_sha256=hashlib.sha256(original.encode()).hexdigest(),generator_sha256=hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),observations=sum(len(s['events']) for s in scenarios)),indent=2)+'\n')
print('Swift observations:',sum(len(s['events']) for s in scenarios))
