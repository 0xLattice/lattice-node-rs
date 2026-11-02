struct Input:Decodable {let name:String;let new:String;let old:String?;let entries:[String:[UInt8]];let counts:[String:Int]}
struct Observation:Encodable {let changes:[String:Int];let entering:[String]}
// Adapters replace only persistence and source lookup, not the walk or header traversal.
actor CountStore {let counts:[String:Int];init(_ counts:[String:Int]){self.counts=counts};func stateCount(_ cid:String,at path:ChainPath) async throws -> Int {counts[cid] ?? 0}}
@main struct Oracle {static func main() async throws {
let rows=try JSONDecoder().decode([Input].self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])))
var output:[String:Observation]=[:]
for row in rows {let storage=NodeStorage(store:CountStore(row.counts),localFetcher:InMemoryContentSource(row.entries.mapValues{Data($0)}));let walk=TipStateWalk(storage:storage,path:["Nexus"]);try await walk.hold(LatticeStateHeader(rawCID:row.new));if let old=row.old{await walk.letGo(LatticeStateHeader(rawCID:old))};output[row.name]=await Observation(changes:walk.changes,entering:walk.entering)}
let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys];print(String(decoding:try encoder.encode(output),as:UTF8.self))
}}
