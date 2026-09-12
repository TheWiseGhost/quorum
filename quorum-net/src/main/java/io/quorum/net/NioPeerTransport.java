package io.quorum.net;

import io.quorum.kv.Transport;
import io.quorum.raft.Message;
import io.quorum.raft.MessageCodec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Raft peer messaging over non-blocking TCP with one selector thread.
 *
 * <p>Each node dials every peer and uses that connection only to send; it receives on the
 * connections peers dial to it. Every outbound queue is bounded: Raft tolerates message loss,
 * so when a peer is slow or down we drop instead of buffering without limit. Broken connections
 * are re-dialled with exponential backoff.
 */
public final class NioPeerTransport implements Transport, AutoCloseable {
    private static final int MAX_QUEUED_BYTES = 64 << 20;

    private final long selfId;
    private final Map<Long, InetSocketAddress> peers;
    private final Consumer<Message> inbound;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final Map<Long, Outbound> outbound = new TreeMap<>();
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final Thread thread;
    private volatile boolean running = true;
    public final AtomicLong sent = new AtomicLong(), received = new AtomicLong(), dropped = new AtomicLong();

    private final class Outbound {
        final long peer;
        final InetSocketAddress addr;
        final ArrayDeque<ByteBuffer> queue = new ArrayDeque<>();
        long queuedBytes;
        SocketChannel ch;
        boolean connected;
        long backoffMs = 50;
        long retryAt;

        Outbound(long peer, InetSocketAddress addr) {
            this.peer = peer;
            this.addr = addr;
        }
    }

    private static final class Inbound {
        final ByteBuffer header = ByteBuffer.allocate(4);
        ByteBuffer body;
        long peer = -1;
    }

    public NioPeerTransport(long selfId, InetSocketAddress listen, Map<Long, InetSocketAddress> peers,
                            Consumer<Message> inbound) throws IOException {
        this.selfId = selfId;
        this.peers = Map.copyOf(peers);
        this.inbound = inbound;
        this.selector = Selector.open();
        this.server = ServerSocketChannel.open();
        server.bind(listen);
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);
        for (Map.Entry<Long, InetSocketAddress> p : peers.entrySet()) {
            if (p.getKey() != selfId) outbound.put(p.getKey(), new Outbound(p.getKey(), p.getValue()));
        }
        thread = Thread.ofPlatform().name("raft-nio-" + selfId).daemon().start(this::loop);
    }

    public int port() {
        return server.socket().getLocalPort();
    }

    /** Thread-safe; called from the node loop. Encoding happens on the caller's thread. */
    @Override
    public void send(Message m) {
        byte[] payload = MessageCodec.encode(m);
        tasks.add(() -> enqueue(m.to(), payload));
        selector.wakeup();
    }

    private void enqueue(long to, byte[] payload) {
        Outbound o = outbound.get(to);
        if (o == null) return;
        if (o.queuedBytes + payload.length > MAX_QUEUED_BYTES) {
            dropped.incrementAndGet();
            return;
        }
        ByteBuffer frame = Frames.frame(payload);
        o.queue.add(frame);
        o.queuedBytes += frame.capacity();
        if (o.connected) o.ch.keyFor(selector).interestOpsOr(SelectionKey.OP_WRITE);
        else maybeConnect(o);
    }

    private void maybeConnect(Outbound o) {
        if (o.ch != null || System.currentTimeMillis() < o.retryAt) return;
        try {
            SocketChannel ch = SocketChannel.open();
            ch.configureBlocking(false);
            ch.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
            ch.connect(o.addr);
            o.ch = ch;
            ch.register(selector, SelectionKey.OP_CONNECT, o);
        } catch (IOException e) {
            fail(o);
        }
    }

    private void fail(Outbound o) {
        if (o.ch != null) {
            try {
                o.ch.close();
            } catch (IOException ignored) {
                // closing a broken channel
            }
        }
        o.ch = null;
        o.connected = false;
        o.queue.clear(); // stale Raft traffic is useless after a reconnect
        o.queuedBytes = 0;
        o.retryAt = System.currentTimeMillis() + o.backoffMs;
        o.backoffMs = Math.min(o.backoffMs * 2, 2_000);
    }

    private void loop() {
        while (running) {
            try {
                selector.select(100);
                Runnable t;
                while ((t = tasks.poll()) != null) t.run();
                for (Outbound o : outbound.values()) if (o.ch == null && !o.queue.isEmpty()) maybeConnect(o);
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();
                    if (!key.isValid()) continue;
                    try {
                        if (key.isAcceptable()) accept();
                        else if (key.isConnectable()) finishConnect(key);
                        else {
                            if (key.isReadable()) read(key);
                            if (key.isValid() && key.isWritable()) write(key);
                        }
                    } catch (IOException e) {
                        if (key.attachment() instanceof Outbound o) fail(o);
                        else closeQuietly(key);
                    }
                }
            } catch (IOException e) {
                if (running) System.err.println("transport loop: " + e);
            }
        }
    }

    private void accept() throws IOException {
        SocketChannel ch = server.accept();
        if (ch == null) return;
        ch.configureBlocking(false);
        ch.setOption(java.net.StandardSocketOptions.TCP_NODELAY, true);
        ch.register(selector, SelectionKey.OP_READ, new Inbound());
    }

    private void finishConnect(SelectionKey key) throws IOException {
        Outbound o = (Outbound) key.attachment();
        if (!o.ch.finishConnect()) return;
        o.connected = true;
        o.backoffMs = 50;
        // Handshake: announce who we are, then stream frames.
        ByteBuffer hello = Frames.frame(java.nio.ByteBuffer.allocate(8).putLong(selfId).array());
        o.queue.addFirst(hello);
        o.queuedBytes += hello.capacity();
        key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
    }

    private void write(SelectionKey key) throws IOException {
        Outbound o = (Outbound) key.attachment();
        while (!o.queue.isEmpty()) {
            ByteBuffer b = o.queue.peekFirst();
            o.ch.write(b);
            if (b.hasRemaining()) return; // socket buffer full; wait for OP_WRITE
            o.queue.pollFirst();
            o.queuedBytes -= b.capacity();
            sent.incrementAndGet();
        }
        key.interestOps(SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException {
        if (key.attachment() instanceof Outbound o) {
            // Outbound connections carry no data back; a read only detects closure.
            if (o.ch.read(ByteBuffer.allocate(64)) < 0) fail(o);
            return;
        }
        Inbound in = (Inbound) key.attachment();
        SocketChannel ch = (SocketChannel) key.channel();
        while (true) {
            if (in.body == null) {
                if (ch.read(in.header) < 0) {
                    closeQuietly(key);
                    return;
                }
                if (in.header.hasRemaining()) return;
                in.header.flip();
                int len = in.header.getInt();
                in.header.clear();
                if (len < 0 || len > Frames.MAX_FRAME) throw new IOException("bad frame " + len);
                in.body = ByteBuffer.allocate(len);
            }
            if (ch.read(in.body) < 0) {
                closeQuietly(key);
                return;
            }
            if (in.body.hasRemaining()) return;
            byte[] payload = in.body.array();
            in.body = null;
            if (in.peer < 0) {
                in.peer = ByteBuffer.wrap(payload).getLong();
            } else {
                received.incrementAndGet();
                inbound.accept(MessageCodec.decode(payload));
            }
        }
    }

    private static void closeQuietly(SelectionKey key) {
        key.cancel();
        try {
            key.channel().close();
        } catch (IOException ignored) {
            // already broken
        }
    }

    @Override
    public void close() {
        running = false;
        selector.wakeup();
        try {
            thread.join(2_000);
            for (Outbound o : outbound.values()) if (o.ch != null) o.ch.close();
            server.close();
            selector.close();
        } catch (IOException | InterruptedException ignored) {
            // shutting down
        }
    }
}
