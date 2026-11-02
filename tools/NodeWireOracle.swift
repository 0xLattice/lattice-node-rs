import Foundation

struct Input: Codable { let name: String; let topic: String; let payload: Data }
struct Output: Codable { let name: String; let topic: String; let payload: Data; let outcome: String; let encoded: Data? }
@main struct Oracle {
    static func main() throws {
        let inputs = try JSONDecoder().decode([Input].self, from: Data(contentsOf: URL(fileURLWithPath: CommandLine.arguments[1])))
        let output = inputs.map { input -> Output in
            var outcome = "unknown"
            var encoded: Data? = nil
            do {
                if let decoded = try ChainSyncWire.decode(topic: input.topic, payload: input.payload) {
                    encoded = try ChainSyncWire.encode(decoded.message, at: decoded.chainPath).payload
                    outcome = "valid"
                }
            } catch let error as OverlayWireError {
                switch error { case .oversized: outcome = "oversized"; case .malformed: outcome = "malformed"; case .nonCanonical: outcome = "noncanonical" }
            } catch { outcome = "unexpected: \(error)" }
            return Output(name: input.name, topic: input.topic, payload: input.payload, outcome: outcome, encoded: encoded)
        }
        let encoder = JSONEncoder(); encoder.outputFormatting = [.sortedKeys]
        FileHandle.standardOutput.write(try encoder.encode(output))
    }
}
