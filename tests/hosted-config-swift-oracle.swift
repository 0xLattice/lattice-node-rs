import Foundation
import Lattice
import cashew
struct Input:Decodable {let name:String;let spec:ChainSpec;let path:[String];let recipient:String?;let entries:[String:[UInt8]];let transactions:[[UInt8]];let materialized:Bool;let modules:[[UInt8]]}
struct Observation:Encodable {let accepted:Bool;var premine:[String]?}
@main struct Oracle {static func main() async throws {
let rows=try JSONDecoder().decode([Input].self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])))
var output:[String:Observation]=[:]
for row in rows {do {let source=InMemoryContentSource(row.entries.mapValues{Data($0)});var txs:[Transaction]=[]
for bytes in row.transactions {let tx=try DagCBOR.decode(Transaction.self,from:Data(bytes));txs.append(row.materialized ? Transaction(signatures:tx.signatures,body:try await tx.body.resolve(fetcher:source)) : tx)}
try NodeRuntime.checkPremine(spec:row.spec,to:row.recipient,transactions:txs,on:row.path,modules:row.modules.map{Data($0)})
let premine=try NodeRuntime.premine(spec:row.spec,to:row.recipient,on:row.path).map{try HeaderImpl<Transaction>(node:$0).rawCID}
output[row.name]=Observation(accepted:true,premine:premine)
}catch{output[row.name]=Observation(accepted:false);FileHandle.standardError.write(Data("\(row.name): \(error)\n".utf8))}}
let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys];print(String(decoding:try encoder.encode(output),as:UTF8.self))
}}
