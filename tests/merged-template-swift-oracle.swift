import Foundation
import Crypto
import Lattice
import UInt256
import cashew
struct PlanEntry:Decodable {let path:[String];let work:UInt256}
struct Input:Decodable {let name:String;let entries:[String:[UInt8]];let tipCID:String;let children:[String:String];let transactions:[[UInt8]];let hosted:Set<String>;let minWork:[PlanEntry]}
struct Observation:Encodable {let built:Bool;var cid:String?;var targets:[UInt256]?;var selected:[String]?;var children:[String]?}
@main struct Oracle {
static func main() async throws {
let rows=try JSONDecoder().decode([Input].self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])))
var output:[String:Observation]=[:]
for row in rows {do {
let source=InMemoryContentSource(row.entries.mapValues{Data($0)})
let previous=try DagCBOR.decode(Block.self,from:Data(row.entries[row.tipCID]!))
let spec=try await previous.spec.resolve(fetcher:source).node!
var txs:[Transaction]=[]
for bytes in row.transactions {let tx=try DagCBOR.decode(Transaction.self,from:Data(bytes));txs.append(Transaction(signatures:tx.signatures,body:try await tx.body.resolve(fetcher:source)))}
let children=try row.children.sorted{$0.key<$1.key}.map{DirectChildCandidate(directory:$0.key,block:try DagCBOR.decode(Block.self,from:Data(row.entries[$0.value]!)))}
let result=try await MiningTemplateAssembly.fit(chainPath:["Nexus"],lifetime:.seconds(60),previous:previous,pooled:txs,provided:children,hosted:row.hosted,parentCarrier:nil,timestamp:previous.timestamp+1,rewardRecipient:nil,minimumWork:Dictionary(uniqueKeysWithValues:row.minWork.map{($0.path,$0.work)}),difficultyAnchor:DifficultyAnchor(blockHeight:previous.height,timestamp:previous.timestamp,target:previous.target),spec:spec,fetcher:source)
let selected=try await MiningTemplateAssembly.blockTransactions(in:result.block,fetcher:source).map{try Mempool.cid(of:$0)}
output[row.name]=Observation(built:true,cid:try BlockHeader(node:result.block).rawCID,targets:result.targets,selected:selected,children:result.childCandidates.map{$0.directory}.sorted())
}catch{output[row.name]=Observation(built:false);FileHandle.standardError.write(Data("\(row.name): \(error)\n".utf8))}}
let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys];print(String(decoding:try encoder.encode(output),as:UTF8.self))
}}
