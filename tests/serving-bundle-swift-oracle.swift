import Foundation
import Lattice
import cashew
struct Input:Decodable {let name:String;let root:String;let entries:[String:[UInt8]]}
actor Collector:VolumeStorer {
 var roots:Set<String>=[]
 func store(volume:SerializedVolume) async throws { roots.insert(volume.root) }
 func result(root:String)->[String] {[root]+roots.filter{$0 != root}.sorted()}
}
@main struct Oracle {
 static func main() async throws {
  let rows=try JSONDecoder().decode([Input].self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])))
  var output:[String:[String]]=[:]
  for row in rows {
   let source=InMemoryContentSource(row.entries.mapValues{Data($0)})
   let collector=Collector()
   do {try await BlockHeader(rawCID:row.root).storeBlock(fetcher:source,storer:collector);output[row.name]=await collector.result(root:row.root)}
   catch {output[row.name]=[row.root]}
  }
  let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys];print(String(decoding:try encoder.encode(output),as:UTF8.self))
 }
}
