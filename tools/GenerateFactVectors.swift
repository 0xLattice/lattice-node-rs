import Foundation
@main struct Generate {
 static func main() throws {
  let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys,.withoutEscapingSlashes]
  for i in 0..<20 {
   let block=ChainBlockFact(blockHash:"block/\(i)",parentBlockHash:i==0 ? nil : "block/\(i-1)",blockHeight:UInt64(i),postStateCID:"post",prevStateCID:"prev",parentStateCID:i%2==0 ? nil:"parent",specCID:"spec",target:UInt256.max.toHexString(),nextTarget:UInt256(12345).toHexString(),timestamp:Int64(i)*1000-10,stateDiff:StateDiff(replaced:i%2==0 ? [:]:["gone":2],created:["new":1]),childCommitments:i%3==0 ? ["Alpha":"child"]:nil)
   let work=ChainWorkFact(blockHash:block.blockHash,contribution:VerifiedWorkContribution(id:"grind/\(i)",work:UInt256(UInt64(i+1))))
   var facts:[ChainFact]=[.block(block),.work(work)];if i%2==0 {facts.append(.validation(ChainValidationFact(blockHash:block.blockHash)))}
   let batch=BlockImportBatch(facts:facts)
   print(String(data:try encoder.encode(batch),encoding:.utf8)!)
   for fact in facts {print(String(data:try encoder.encode(fact.id),encoding:.utf8)!)}
  }
  print(String(data:try encoder.encode(BlockImportBatch.validation(blockHash:"deferred")),encoding:.utf8)!)
  print(String(data:try encoder.encode(ChainFact.validation(ChainValidationFact(blockHash:"deferred")).id),encoding:.utf8)!)
  print(String(data:try encoder.encode(BlockImportBatch(facts:[.exclusion(ChainExclusionFact(blockHash:"excluded"))])),encoding:.utf8)!)
  print(String(data:try encoder.encode(ChainFact.exclusion(ChainExclusionFact(blockHash:"excluded")).id),encoding:.utf8)!)
 }
}
