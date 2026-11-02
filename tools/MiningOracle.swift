import Foundation
import Lattice
import cashew
struct Input:Decodable {let spec:ChainSpec;let bodies:[TransactionBody];let commands:[Command];let block:Data}
struct Command:Decodable {let op:String;let index:Int?;let id:UInt64?;let value:String?;let now:Int64}
@main struct Oracle {
 static func main() throws {
  let input=try JSONDecoder().decode(Input.self,from:Data(contentsOf:URL(fileURLWithPath:CommandLine.arguments[1])))
  let transactions=try input.bodies.map {Transaction(signatures:[:],body:try HeaderImpl(node:$0))};let block=Block(data:input.block)!
  let peer=PeerID(key:"peer",session:1)
  var state=MiningState(tipCID:"A",spec:input.spec,config:MiningConfig(maxPendingPeerAdmissions:2,maxPendingPerPeer:1,maxPendingReturned:2,maxPendingSubmitted:1,maxReissues:2,maxWaitingTemplateRequests:2))
  var preflights:[PreflightJob]=[];var builds:[TemplateJob]=[];var rows:[[String:Any]]=[]
  func job(_ j:PreflightJob)->[String:Any] {["kind":"preflight","cid":j.cid,"epoch":j.tipEpoch,"tip":j.tipCID,"version":j.poolVersion,"policies":j.checksPolicies]}
  for command in input.commands {
   var event:MiningEvent?;let index=command.index ?? 0;let id=command.id ?? 0
   switch command.op {
   case "receive":let origin:TransactionOrigin = switch command.value {case "local":.local(replyID:id);case "submitted":.submitted(replyID:id);case "restored":.restored(addedAt:1000);case "returned":.returned;default:.peer(peer)};event = .transactionReceived(transactions[index % transactions.count],origin:origin)
   case "verdict":if index<preflights.count {let disposition:MempoolDisposition = switch command.value {case "future":.future;case "invalid":.invalid;case "unavailable":.unavailable;default:.ready};event = .preflighted(preflights[index],disposition,balances:["a":200,"b":200,"c":200])}
   case "tip":event = .tipMoved(TipMove(tipCID:command.value ?? "A"))
   case "confirm":event = .tipMoved(TipMove(tipCID:command.value ?? "B",confirmed:[try Mempool.cid(of:transactions[index % transactions.count])]))
   case "template":event = .templateRequested(replyID:id,TemplateRequest(rewardRecipient:command.value))
   case "built":if index<builds.count {let build:TemplateBuild? = command.value == "fail" ? nil : TemplateBuild(workID:"w\(index)",block:block,searchTarget:block.target,targets:[block.target]);event = .templateBuilt(builds[index],build)}
   case "submit":event = .submitWork(replyID:id,workID:"w\(index)",nonce:0)
   case "refuse":event = .policyRefused(tipCID:command.value ?? "A",cids:[try Mempool.cid(of:transactions[index % transactions.count])])
   default:fatalError()
   }
   let effects=event.map {state.step($0,now:command.now)} ?? []
   let encoded=try effects.map {effect -> [String:Any] in switch effect {
    case .poolChanged(let d):return ["kind":"pool","added":d.added.map(\.cid),"journaled":d.journaled.map(\.cid),"removed":d.removed]
    case .transactionAdmitted(let id,let cid,let count,let bytes):return ["kind":"admitted","id":id,"cid":cid,"count":count,"bytes":bytes]
    case .transactionRefused(let id,let error):return ["kind":"refused","id":id,"error":String(describing:error)]
    case .announceTransaction(let cid):return ["kind":"announce","cid":cid]
    case .preflight(let j):preflights.append(j);return job(j)
    case .buildTemplate(let j):builds.append(j);return ["kind":"build","id":j.id,"epoch":j.tipEpoch,"tip":j.tipCID,"version":j.poolVersion,"transactions":try j.transactions.map {try Mempool.cid(of:$0)}]
    case .templateIssued(let id,let t):return ["kind":"issued","id":id,"work":t.workID,"expires":t.expiresAt,"tip":t.tipCID,"version":t.poolVersion]
    case .templateRefused(let id,let error):return ["kind":"templateRefused","id":id,"error":String(describing:error)]
    case .workRefused(let id,let error):return ["kind":"workRefused","id":id,"error":String(describing:error)]
    case .mined(let id,let b):return ["kind":"mined","id":id,"cid":try BlockHeader(node:b).rawCID]
    case .returnTransactions(let left,let carried):return ["kind":"returned","left":left,"carried":carried.sorted()]
   }}
   rows.append(["effects":encoded,"tip":state.tipCID,"epoch":state.tipEpoch,"items":state.mempool.items.map(\.cid),"version":state.mempool.version,"journaled":state.journaled.sorted(),"pending":state.pendingAdmissions,"origins":state.pendingOrigins,"peer":state.pendingPeerAdmissions,"submitted":state.pendingSubmitted,"returned":state.pendingReturned,"waiting":state.waitingTemplateRequests,"outstanding":state.outstandingPreflights,"templates":state.templates.count])
  }
  FileHandle.standardOutput.write(try JSONSerialization.data(withJSONObject:rows,options:[.sortedKeys]))
 }
}
