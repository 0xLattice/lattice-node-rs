import Foundation
import Crypto
import Lattice
import UInt256
import cashew
struct Anchor:Decodable{let height:UInt64;let timestamp:Int64;let target:UInt256}
struct PlanEntry:Decodable{let path:[String];let work:UInt256}
struct Input:Decodable{let name:String;let anchor:Anchor?;let materializeBodies:Bool;let entries:[String:[UInt8]];let tipCID:String;let transactions:[[UInt8]];let now:Int64;let recipient:String?;let minWork:[PlanEntry]}
struct Observation:Encodable{let built:Bool;var cid:String?;var workID:String?;var searchTarget:UInt256?;var targets:[UInt256]?;var digest:String?;var refused:[String]?;var selected:[String]?}
@main struct Oracle {static func main() async throws {let rows=try JSONDecoder().decode([Input].self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])));var output:[String:Observation]=[:]
for row in rows {do {let source=InMemoryContentSource(row.entries.mapValues{Data($0)});let previous=try DagCBOR.decode(Block.self,from:Data(row.entries[row.tipCID]!));let spec=try await previous.spec.resolve(fetcher:source).node!;var txs=try row.transactions.map{try DagCBOR.decode(Transaction.self,from:Data($0))};if row.materializeBodies {var loaded:[Transaction]=[];for tx in txs {loaded.append(Transaction(signatures:tx.signatures,body:try await tx.body.resolve(fetcher:source)))};txs=loaded};let (minimum,overflow)=previous.timestamp.addingReportingOverflow(1);guard !overflow else {throw NodeAPIError.timestampOverflow};let timestamp=max(minimum,row.now)
let filtered=await MiningPlan.policyAcceptedTransactions(txs,chainPath:["Nexus"],previous:previous,timestamp:timestamp,spec:spec,fetcher:source)
let result=try await MiningTemplateAssembly.fit(chainPath:["Nexus"],lifetime:.seconds(60),previous:previous,pooled:filtered.accepted,provided:[],hosted:[],parentCarrier:nil,timestamp:timestamp,rewardRecipient:row.recipient,minimumWork:Dictionary(uniqueKeysWithValues:row.minWork.map{($0.path,$0.work)}),difficultyAnchor:row.anchor.map{DifficultyAnchor(blockHeight:$0.height,timestamp:$0.timestamp,target:$0.target)},spec:spec,fetcher:source)
let selected=try await MiningTemplateAssembly.blockTransactions(in:result.block,fetcher:source).map{try Mempool.cid(of:$0)};let digest=NodeRuntime.templateDigest(tip:row.tipCID,transactions:try txs.map{try Mempool.cid(of:$0)})
output[row.name]=Observation(built:true,cid:try BlockHeader(node:result.block).rawCID,workID:result.workID,searchTarget:result.searchTarget,targets:result.targets,digest:digest,refused:filtered.refused,selected:selected)
}catch{output[row.name]=Observation(built:false);FileHandle.standardError.write(Data("\(row.name): \(error)\n".utf8))}}
let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys];print(String(decoding:try encoder.encode(output),as:UTF8.self))}}
