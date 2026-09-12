package io.quorum.net;

import io.quorum.kv.ClientCodec;
import io.quorum.kv.Request;
import io.quorum.kv.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Client-facing server: one virtual thread reads each connection and another writes its
 * responses, so thousands of mostly idle clients cost almost nothing. Requests are pipelined:
 * responses carry the request id and may come back out of order.
 */
public final class ClientServer implements AutoCloseable {
    private final ServerSocket socket;
    private final BiConsumer<Request, Consumer<Response>> handler;
    private final RequestMetrics metrics;
    private volatile boolean running = true;

    /**
     * @param handler called on a virtual thread; must hand the request to the node loop and
     *                invoke the callback (from any thread) exactly once
     */
    public ClientServer(InetSocketAddress listen, BiConsumer<Request, Consumer<Response>> handler, RequestMetrics metrics)
            throws IOException {
        this.socket = new ServerSocket();
        socket.setReuseAddress(true);
        socket.bind(listen);
        this.handler = handler;
        this.metrics = metrics;
        Thread.ofVirtual().name("client-accept").start(this::acceptLoop);
    }

    public int port() {
        return socket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = socket.accept();
                s.setTcpNoDelay(true);
                Thread.ofVirtual().name("client-conn").start(() -> serve(s));
            } catch (IOException e) {
                if (running) System.err.println("accept: " + e);
            }
        }
    }

    private static final byte[] POISON = new byte[0];

    private void serve(Socket s) {
        LinkedBlockingQueue<byte[]> responses = new LinkedBlockingQueue<>();
        Thread writer = Thread.ofVirtual().name("client-writer").start(() -> {
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))) {
                while (true) {
                    byte[] r = responses.take();
                    if (r == POISON) return;
                    Frames.write(out, r);
                    if (responses.isEmpty()) out.flush(); // batch flushes under load
                }
            } catch (IOException | InterruptedException e) {
                // connection gone
            }
        });
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()))) {
            byte[] frame;
            while ((frame = Frames.read(in)) != null) {
                ClientCodec.Framed<Request> req = ClientCodec.decodeRequest(frame);
                long start = System.nanoTime();
                String kind = req.body().getClass().getSimpleName();
                handler.accept(req.body(), resp -> {
                    metrics.record(kind, resp.code(), System.nanoTime() - start);
                    responses.add(ClientCodec.encodeResponse(req.requestId(), resp));
                });
            }
        } catch (IOException | RuntimeException e) {
            // client disconnected or sent garbage
        } finally {
            responses.add(POISON);
            try {
                writer.join(1_000);
                s.close();
            } catch (IOException | InterruptedException ignored) {
                // closing
            }
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        socket.close();
    }
}
