import Foundation
import Lattice
import cashew
struct Limits: Codable {let count:Int;let bytes:Int;let signatures:Int;let floor:UInt64}
struct Operation: Codable {let op:String;let body:TransactionBody?;let balances:[String:UInt64]?;let at:Int64?;let remove:[Int]?}
struct Scenario: Codable {let name:String;let spec:ChainSpec;let limits:Limits;let operations:[Operation]}
struct Projection: Codable {let count:Int;let bytes:Int;let fee:String;let weight:Int;let feeBytes:Int;let full:Bool}
struct Observation: Codable {let result:String;let items:[String];let bytes:Int;let version:UInt64;let next:[String];let projected:[Projection];let replaced:[String];let evicted:[String];let removed:[String]}
struct Output: Codable {let scenario:Scenario;let observations:[Observation]}
@main struct Oracle {
 static func main() throws {
 let cases=try JSONDecoder().decode([Scenario].self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])))
 var outputs:[Output]=[]
 for input in cases {
  var pool=Mempool(limits:MempoolLimits(maxCount:input.limits.count,maxBytes:input.limits.bytes,maxSignatures:input.limits.signatures,minRelayFee:input.limits.floor));var submitted:[String]=[];var observations:[Observation]=[]
  for op in input.operations {
   var result="ok";var mutation=MempoolMutation()
   do { switch op.op {
    case "submit":let tx=Transaction(signatures:[:],body:try HeaderImpl(node:op.body!));submitted.append(try Mempool.cid(of:tx));mutation=try pool.submit(tx,spec:input.spec,addedAt:op.at ?? 0,balances:op.balances ?? [:])
    case "rebalance":mutation=pool.rebalance(op.balances ?? [:])
    case "remove":mutation=pool.remove((op.remove ?? []).compactMap { $0 < submitted.count ? submitted[$0] : nil })
    case "clear":mutation=pool.clear()
    default:fatalError()
   }} catch {result=String(describing:error)}
   observations.append(Observation(result:result,items:pool.items.map(\.cid),bytes:pool.byteCount,version:pool.version,next:try pool.nextBlock(input.spec).map {try Mempool.cid(of:$0)},projected:pool.projectedBlocks(count:4,spec:input.spec).map {Projection(count:$0.transactionCount,bytes:$0.bytes,fee:$0.lowestFee.description,weight:$0.lowestFeeWeight,feeBytes:$0.lowestFeeBytes,full:$0.full)},replaced:mutation.replaced.map(\.cid),evicted:mutation.evicted.map(\.cid),removed:mutation.removed.map(\.cid)))
  }
  outputs.append(Output(scenario:input,observations:observations))
 }
 let encoder=JSONEncoder();encoder.outputFormatting=[.sortedKeys];FileHandle.standardOutput.write(try encoder.encode(outputs))
 }
}
