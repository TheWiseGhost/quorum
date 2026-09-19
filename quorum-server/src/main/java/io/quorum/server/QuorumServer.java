package io.quorum.server;

import com.sun.net.httpserver.HttpServer;
import io.quorum.common.ExecutorScheduler;
import io.quorum.common.RealFileSystem;
import io.quorum.common.Rng;
import io.quorum.kv.KvNode;
import io.quorum.kv.NodeConfig;
import io.quorum.kv.Request;
import io.quorum.kv.Response;
import io.quorum.net.ClientServer;
import io.quorum.net.NioPeerTransport;
import io.quorum.net.RequestMetrics;
import io.quorum.raft.ConfState;
import io.quorum.storage.EngineStats;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * One Quorum server process: the {@link KvNode} on a single-threaded node loop, an NIO peer
 * transport, a virtual-thread client server, and an HTTP endpoint for {@code /status} and
 * Prometheus {@code /metrics}.
 */
public final class QuorumServer implements AutoCloseable {
    private final ServerConfig cfg;
    private final ExecutorScheduler loop;
    private final ExecutorScheduler background;
    private final NioPeerTransport transport;
    private final KvNode node;
    private final ClientServer clients;
    private final RequestMetrics metrics = new RequestMetrics();
    private final HttpServer http;

    public QuorumServer(ServerConfig cfg) throws IOException {
        this(cfg, nc -> nc);
    }

    /** @param tweak adjusts the node configuration (benchmark ablations) */
    public QuorumServer(ServerConfig cfg, java.util.function.UnaryOperator<NodeConfig> tweak) throws IOException {
        this.cfg = cfg;
        this.loop = new ExecutorScheduler("node-loop-" + cfg.id(), 1);
        this.background = new ExecutorScheduler("lsm-bg-" + cfg.id(), 2);
        RealFileSystem fs = new RealFileSystem(cfg.dataDir());
        KvNode[] holder = new KvNode[1];
        this.transport = new NioPeerTransport(cfg.id(), cfg.peers().get(cfg.id()), cfg.peers(),
                m -> loop.execute(() -> {
                    KvNode n = holder[0];
                    if (n != null) n.onMessage(m); // messages before startup are dropped (Raft retries)
                }));
        ConfState bootstrap = cfg.bootstrap().isEmpty() ? ConfState.EMPTY : new ConfState(cfg.bootstrap(), new TreeSet<>());
        NodeConfig nc = tweak.apply(new NodeConfig(cfg.id(), bootstrap, io.quorum.raft.RaftConfig.defaults(cfg.id()),
                io.quorum.storage.Options.defaults(), cfg.tickMs() * 1_000_000L, cfg.snapshotEvery(), 64 << 20,
                io.quorum.common.PlantedBug.NONE));
        this.node = onLoop(() -> new KvNode(nc, fs, loop, background, transport, Rng.seeded(System.nanoTime() ^ cfg.id()),
                new KvNode.Observer() {}));
        holder[0] = node;
        loop.execute(node::start);
        this.clients = new ClientServer(cfg.clientAddress(), this::handle, metrics);
        this.http = cfg.metricsPort() > 0 ? startHttp(cfg.metricsPort()) : null;
    }

    private void handle(Request req, Consumer<Response> cb) {
        loop.execute(() -> node.submit(req, cb));
    }

    private <T> T onLoop(Supplier<T> s) {
        CompletableFuture<T> f = new CompletableFuture<>();
        loop.execute(() -> {
            try {
                f.complete(s.get());
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        try {
            return f.get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("node loop failed", e);
        }
    }

    public int clientPort() {
        return clients.port();
    }

    public long id() {
        return cfg.id();
    }

    public String status() {
        return onLoop(node::status);
    }

    public boolean isLeader() {
        return onLoop(() -> node.raft().isLeader());
    }

    public String prometheus() {
        return onLoop(() -> {
            StringBuilder sb = new StringBuilder();
            var raft = node.raft();
            gauge(sb, "quorum_term", raft.term());
            gauge(sb, "quorum_is_leader", raft.isLeader() ? 1 : 0);
            gauge(sb, "quorum_commit_index", raft.commitIndex());
            gauge(sb, "quorum_applied_index", node.stateMachine().appliedIndex());
            gauge(sb, "quorum_last_log_index", raft.lastIndex());
            gauge(sb, "quorum_snapshot_index", raft.snapshotIndex());
            gauge(sb, "quorum_voters", raft.conf().voters().size());
            for (long peer : raft.conf().voters()) {
                long match = raft.matchIndex(peer);
                if (raft.isLeader() && match >= 0) {
                    sb.append("quorum_replication_lag{peer=\"").append(peer).append("\"} ")
                            .append(raft.lastIndex() - match).append('\n');
                }
            }
            EngineStats st = node.stateMachine().engine().stats();
            gauge(sb, "quorum_lsm_write_amplification", st.writeAmplification());
            gauge(sb, "quorum_lsm_live_bytes", st.liveTableBytes());
            gauge(sb, "quorum_lsm_compactions_total", st.compactions());
            gauge(sb, "quorum_lsm_block_cache_hits_total", st.blockCacheHits());
            gauge(sb, "quorum_lsm_block_cache_misses_total", st.blockCacheMisses());
            for (int l = 0; l < st.filesPerLevel().length; l++) {
                sb.append("quorum_lsm_files{level=\"").append(l).append("\"} ").append(st.filesPerLevel()[l]).append('\n');
            }
            gauge(sb, "quorum_peer_messages_sent_total", transport.sent.get());
            gauge(sb, "quorum_peer_messages_received_total", transport.received.get());
            gauge(sb, "quorum_peer_messages_dropped_total", transport.dropped.get());
            metrics.appendPrometheus(sb);
            return sb.toString();
        });
    }

    private static void gauge(StringBuilder sb, String name, double v) {
        sb.append(name).append(' ').append(v == Math.rint(v) ? Long.toString((long) v) : Double.toString(v)).append('\n');
    }

    private HttpServer startHttp(int port) throws IOException {
        HttpServer s = HttpServer.create(new InetSocketAddress(port), 0);
        s.createContext("/status", ex -> respond(ex, "application/json", status()));
        s.createContext("/metrics", ex -> respond(ex, "text/plain; version=0.0.4", prometheus()));
        s.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        s.start();
        return s;
    }

    private static void respond(com.sun.net.httpserver.HttpExchange ex, String type, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.sendResponseHeaders(200, b.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(b);
        }
    }

    @Override
    public void close() {
        if (http != null) http.stop(0);
        try {
            clients.close();
        } catch (IOException ignored) {
            // shutting down
        }
        transport.close();
        CompletableFuture<Void> done = new CompletableFuture<>();
        loop.execute(() -> {
            node.close();
            done.complete(null);
        });
        try {
            done.get(10, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best effort
        }
        loop.close();
        background.close();
    }

    public static void main(String[] args) throws Exception {
        ServerConfig cfg = ServerConfig.parse(args);
        QuorumServer server = new QuorumServer(cfg);
        System.out.printf("quorum node %d: peers on %s, clients on %s%s, data in %s%n", cfg.id(),
                cfg.peers().get(cfg.id()), cfg.clientAddress(),
                cfg.metricsPort() > 0 ? ", metrics on :" + cfg.metricsPort() : "", cfg.dataDir().toAbsolutePath());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
