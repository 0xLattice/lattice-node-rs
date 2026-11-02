// Runs unchanged pinned Swift runtime and peer-memory implementations. The
// fixture disables STUN, public seeds and health probes; all dials are loopback.
import Foundation
import Ivy
@testable import LatticeNode

@main struct ChildPeerMemoryOracle {
    static let paths = [["Nexus", "Alpha"], ["Nexus", "Alpha", "Leaf"]]
    static var leaf: [String] { paths[1] }
    struct Host {
        let configuration: NodeConfiguration
        let overlay: IvyConfig
        var endpoint: PeerEndpoint {
            PeerEndpoint(publicKey: configuration.processPublicKey, host: "127.0.0.1", port: configuration.listenPort)
        }
        func start() async throws -> NodeRuntime {
            try await NodeRuntime.start(storage: try await NodeStorage.open(configuration: configuration), configuration: configuration, overlay: overlay)
        }
    }
    static func host(_ directory: URL, _ index: Int, _ port: UInt16, _ seeds: [PeerEndpoint]) throws -> Host {
        let configuration = try NodeConfiguration(
            chainPath: ["Nexus"], storagePath: directory,
            privateKeyHex: String(repeating: String(format: "%02x", 101 + index), count: 32),
            listenPort: port, rpcPort: port == 65535 ? port - 1 : port + 1, webSocket: .off,
            childSeedPeers: seeds.isEmpty ? [:] : [leaf: seeds], peerSearchInterval: 1,
            hostedChildren: paths
        )
        let overlay = IvyConfig(
            signingKey: configuration.signingKey, listenPort: port, bootstrapPeers: [],
            requestTimeout: .seconds(5), stunServers: [], healthConfig: PeerHealthConfig(enabled: false),
            maxConnectionsPerNetgroup: 64, mode: .overlay
        )
        return Host(configuration: configuration, overlay: overlay)
    }
    static func eventually(_ label: String, _ condition: () async throws -> Bool) async throws {
        for _ in 0..<600 {
            if try await condition() { return }
            try await Task.sleep(for: .milliseconds(50))
        }
        throw NSError(domain: "ChildPeerMemoryOracle", code: 1, userInfo: [NSLocalizedDescriptionKey: label])
    }
    static func main() async throws {
        let directory = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
        let ports = CommandLine.arguments[2...].map { UInt16($0)! }
        let hosts = try (0..<3).map { try host(directory.appendingPathComponent("standing-\($0)"), $0, ports[$0], []) }
        var standing: [NodeRuntime] = []
        for host in hosts { standing.append(try await host.start()) }
        let initial = Array(hosts.prefix(2).map(\.endpoint))
        let expected = Set(initial.map(\.publicKey))
        let seekerDirectory = directory.appendingPathComponent("seeker")
        func firstRun() async throws {
            let node = try await host(seekerDirectory, 3, ports[3], initial).start()
            try await eventually("child-only seed peers not reached") { Set(await node.ivy.connectedPeers.map(\.publicKey)) == expected }
            try await eventually("all initial chain books not populated") {
                guard let memory = node.peerMemory else { return false }
                for path in [["Nexus"]] + paths {
                    if Set(await memory.remembered(for: path).map(\.publicKey)) != expected { return false }
                }
                return true
            }
            try await eventually("all ancestry books not saved") {
                guard let data = try? Data(contentsOf: seekerDirectory.appendingPathComponent("peers.json")),
                      let json = try JSONSerialization.jsonObject(with: data) as? [String: Any],
                      let chains = json["chains"] as? [String: [[String: Any]]] else { return false }
                return ([ ["Nexus"] ] + paths).allSatisfy { chains[$0.joined(separator: "/")]?.count == 2 }
            }
            try await node.host(paths, childSeedPeers: [leaf: [hosts[2].endpoint]])
            try await Task.sleep(for: .milliseconds(1200))
            guard await standing[2].ivy.connectedPeers.isEmpty else { throw NSError(domain: "seed-update-dialled", code: 1) }
            await node.stop()
        }
        try await firstRun()
        var restartSeedContacted = false
        func secondRun() async throws {
            let node = try await host(seekerDirectory, 3, ports[3], [hosts[2].endpoint]).start()
            try await eventually("remembered peers did not reconnect") {
                expected.isSubset(of: Set(await node.ivy.connectedPeers.map(\.publicKey)))
            }
            try await Task.sleep(for: .milliseconds(1200))
            restartSeedContacted = !(await standing[2].ivy.connectedPeers.isEmpty)
            guard let memory = node.peerMemory else { throw NSError(domain: "missing-memory", code: 1) }
            for path in [["Nexus"]] + paths {
                guard expected.isSubset(of: Set(await memory.remembered(for: path).map(\.publicKey))) else { throw NSError(domain: "child-book-lost", code: 1) }
            }
            await node.stop()
        }
        try await secondRun()
        await standing[0].stop(); await standing[1].stop()
        func thirdRun() async throws {
            let node = try await host(seekerDirectory, 3, ports[3], [hosts[2].endpoint]).start()
            try await eventually("child seed fallback did not run") { Set(await node.ivy.connectedPeers.map(\.publicKey)) == [hosts[2].endpoint.publicKey] }
            await node.stop()
        }
        try await thirdRun()
        await standing[2].stop()
        func isolatedRun() async throws {
            let node = try await host(seekerDirectory, 3, ports[3], []).start()
            guard node.peerMemory == nil else { throw NSError(domain: "isolated-memory-enabled", code: 1) }
            try await Task.sleep(for: .milliseconds(1200))
            guard await node.ivy.connectedPeers.isEmpty else { throw NSError(domain: "isolated-dialled", code: 1) }
            await node.stop()
        }
        try await isolatedRun()
        print("{\"passed\":true,\"childOnlyMemory\":true,\"ancestorBooks\":3,\"seedUpdateSuppressed\":true,\"rememberedReconnect\":true,\"restartSeedContacted\":\(restartSeedContacted),\"fallback\":true,\"isolated\":true}")
    }
}
