package io.quorum.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Starts an N-node cluster in this JVM on localhost (integration tests and benchmarks). */
public final class LocalCluster implements AutoCloseable {
    private final Map<Long, QuorumServer> servers = new TreeMap<>();
    private final Map<Long, ServerConfig> configs = new TreeMap<>();
    private final java.util.function.UnaryOperator<io.quorum.kv.NodeConfig> tweak;

    public LocalCluster(int n, Path dataRoot, long tickMs) throws IOException {
        this(n, dataRoot, tickMs, nc -> nc);
    }

    public LocalCluster(int n, Path dataRoot, long tickMs, java.util.function.UnaryOperator<io.quorum.kv.NodeConfig> tweak)
            throws IOException {
        this.tweak = tweak;
        Map<Long, InetSocketAddress> peers = new TreeMap<>();
        TreeSet<Long> voters = new TreeSet<>();
        for (long id = 1; id <= n; id++) {
            peers.put(id, new InetSocketAddress("127.0.0.1", freePort()));
            voters.add(id);
        }
        for (long id = 1; id <= n; id++) {
            configs.put(id, new ServerConfig(id, peers, new InetSocketAddress("127.0.0.1", freePort()), 0,
                    dataRoot.resolve("n" + id), voters, tickMs, 50_000));
        }
        for (long id = 1; id <= n; id++) start(id);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    public void start(long id) throws IOException {
        servers.put(id, new QuorumServer(configs.get(id), tweak));
    }

    public void stop(long id) {
        QuorumServer s = servers.remove(id);
        if (s != null) s.close();
    }

    public List<InetSocketAddress> clientAddresses() {
        List<InetSocketAddress> out = new ArrayList<>();
        for (ServerConfig c : configs.values()) out.add(c.clientAddress());
        return out;
    }

    public QuorumServer server(long id) {
        return servers.get(id);
    }

    /** Waits for a leader and returns its id. */
    public long awaitLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (QuorumServer s : servers.values()) if (s.isLeader()) return s.id();
            Thread.sleep(20);
        }
        StringBuilder sb = new StringBuilder("no leader within " + timeoutMs + "ms:");
        for (QuorumServer s : servers.values()) sb.append("\n  ").append(s.status());
        throw new IllegalStateException(sb.toString());
    }

    @Override
    public void close() {
        for (QuorumServer s : servers.values()) s.close();
        servers.clear();
    }
}
