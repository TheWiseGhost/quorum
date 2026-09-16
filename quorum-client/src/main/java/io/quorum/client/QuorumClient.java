package io.quorum.client;

import io.quorum.kv.ClientCodec;
import io.quorum.kv.Command;
import io.quorum.kv.KvStateMachine;
import io.quorum.kv.Request;
import io.quorum.kv.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous client with leader discovery, retries and exactly-once writes.
 *
 * <p>Every write carries {@code (sessionId, seq)}. The server remembers only the last
 * sequence per session, so each session has at most one write in flight. Concurrency comes
 * from a pool of sessions: a write borrows one, retries with the same seq until it gets a
 * definite answer, then returns it.
 */
public final class QuorumClient implements AutoCloseable {
    public record CasResult(boolean ok, byte[] current) {}

    private static final SecureRandom RANDOM = new SecureRandom();

    private final List<InetSocketAddress> servers;
    private final long timeoutMs;
    private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<Session> sessions = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("quorum-client-timer").daemon().factory());
    private final AtomicInteger leader = new AtomicInteger(0);
    private final AtomicLong requestIds = new AtomicLong();
    private volatile boolean closed;

    private static final class Session {
        final long id = RANDOM.nextLong();
        long seq;
    }

    private final class Connection {
        final Socket socket;
        final DataOutputStream out;
        final Map<Long, CompletableFuture<Response>> pending = new ConcurrentHashMap<>();
        volatile boolean dead;

        Connection(InetSocketAddress addr) throws IOException {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.connect(addr, 1_000);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            Thread.ofVirtual().name("quorum-client-reader").start(this::readLoop);
        }

        private void readLoop() {
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()))) {
                byte[] frame;
                while ((frame = io.quorum.net.Frames.read(in)) != null) {
                    ClientCodec.Framed<Response> r = ClientCodec.decodeResponse(frame);
                    CompletableFuture<Response> f = pending.remove(r.requestId());
                    if (f != null) f.complete(r.body());
                }
            } catch (IOException | RuntimeException e) {
                // fall through
            }
            dead = true;
            IOException gone = new IOException("connection closed");
            pending.values().forEach(f -> f.completeExceptionally(gone));
            pending.clear();
        }

        CompletableFuture<Response> send(Request req) {
            long id = requestIds.incrementAndGet();
            CompletableFuture<Response> f = new CompletableFuture<>();
            pending.put(id, f);
            byte[] payload = ClientCodec.encodeRequest(id, req);
            try {
                synchronized (out) {
                    io.quorum.net.Frames.write(out, payload);
                    out.flush();
                }
            } catch (IOException e) {
                pending.remove(id);
                dead = true;
                f.completeExceptionally(e);
            }
            return f;
        }

        void close() {
            dead = true;
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing
            }
        }
    }

    public QuorumClient(List<InetSocketAddress> servers) {
        this(servers, 10_000);
    }

    public QuorumClient(List<InetSocketAddress> servers, long timeoutMs) {
        if (servers.isEmpty()) throw new IllegalArgumentException("no servers");
        this.servers = List.copyOf(servers);
        this.timeoutMs = timeoutMs;
    }

    // ------------------------------------------------------------------ public API

    public CompletableFuture<byte[]> get(byte[] key) {
        return call(new Request.Get(key)).thenApply(Response::value);
    }

    public CompletableFuture<List<KvStateMachine.Row>> scan(byte[] from, byte[] to, int limit) {
        return call(new Request.Scan(from, to, limit)).thenApply(Response::rows);
    }

    public CompletableFuture<Void> put(byte[] key, byte[] value) {
        return write(s -> new Command.Put(s.id, s.seq, key, value)).thenApply(r -> null);
    }

    public CompletableFuture<Void> delete(byte[] key) {
        return write(s -> new Command.Delete(s.id, s.seq, key)).thenApply(r -> null);
    }

    /** Sets {@code key} to {@code newValue} iff it currently equals {@code expected} (null = absent). */
    public CompletableFuture<CasResult> cas(byte[] key, byte[] expected, byte[] newValue) {
        return write(s -> new Command.Cas(s.id, s.seq, key, expected, newValue))
                .thenApply(r -> new CasResult(r.ok(), r.value()));
    }

    public CompletableFuture<String> status(int serverIndex) {
        return sendTo(serverIndex, new Request.Status()).thenApply(Response::message);
    }

    public CompletableFuture<String> admin(Request req) {
        return call(req).thenApply(Response::message);
    }

    // ------------------------------------------------------------------ plumbing

    private CompletableFuture<Response> write(java.util.function.Function<Session, Command> make) {
        Session s = sessions.poll();
        if (s == null) s = new Session();
        Session session = s;
        session.seq++;
        Command cmd = make.apply(session);
        return call(new Request.Write(cmd)).whenComplete((r, e) -> {
            // On a definite outcome the session is reusable. If we gave up, the command may
            // still apply later, so the session is discarded rather than reused.
            if (e == null) sessions.add(session);
        }).thenApply(r -> {
            if (!r.ok() && cmd instanceof Command.Cas == false) throw new IllegalStateException("write rejected: " + r.message());
            return r;
        });
    }

    private CompletableFuture<Response> call(Request req) {
        CompletableFuture<Response> result = new CompletableFuture<>();
        attempt(req, result, System.currentTimeMillis() + timeoutMs, leader.get(), 0);
        return result;
    }

    private void attempt(Request req, CompletableFuture<Response> result, long deadline, int target, int tries) {
        if (closed) {
            result.completeExceptionally(new IllegalStateException("client closed"));
            return;
        }
        if (System.currentTimeMillis() > deadline) {
            result.completeExceptionally(new TimeoutException("no definite answer for " + req.getClass().getSimpleName()
                    + " within " + timeoutMs + "ms"));
            return;
        }
        sendTo(target, req).orTimeout(Math.min(2_000, timeoutMs), TimeUnit.MILLISECONDS).whenComplete((resp, err) -> {
            if (err != null) {
                retryLater(req, result, deadline, (target + 1) % servers.size(), tries);
                return;
            }
            switch (resp.code()) {
                case OK -> {
                    leader.set(target);
                    result.complete(resp);
                }
                case NOT_LEADER -> {
                    int next = resp.leaderHint() > 0 ? indexOfNode(resp.leaderHint()) : (target + 1) % servers.size();
                    retryLater(req, result, deadline, next, tries);
                }
                case RETRY -> retryLater(req, result, deadline, target, tries);
                case ERROR -> result.completeExceptionally(new IllegalStateException(resp.message()));
            }
        });
    }

    /** Node ids are 1-based positions in the server list by convention. */
    private int indexOfNode(long nodeId) {
        int i = (int) nodeId - 1;
        return i >= 0 && i < servers.size() ? i : 0;
    }

    private void retryLater(Request req, CompletableFuture<Response> result, long deadline, int target, int tries) {
        long backoff = Math.min(200, 5L << Math.min(tries, 5));
        timer.schedule(() -> attempt(req, result, deadline, target, tries + 1), backoff, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<Response> sendTo(int index, Request req) {
        try {
            Connection c = connections.compute(index, (i, existing) -> {
                if (existing != null && !existing.dead) return existing;
                try {
                    return new Connection(servers.get(i));
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            return c.send(req);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() {
        closed = true;
        connections.values().forEach(Connection::close);
        timer.shutdownNow();
    }
}
