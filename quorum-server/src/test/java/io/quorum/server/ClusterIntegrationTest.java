package io.quorum.server;

import io.quorum.check.History;
import io.quorum.check.LinearizabilityChecker;
import io.quorum.check.Op;
import io.quorum.client.QuorumClient;
import io.quorum.common.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/** Real sockets, real fsync, real threads: a 3-node cluster on localhost. */
class ClusterIntegrationTest {
    @Test
    void basicOperationsAndFailover(@TempDir Path tmp) throws Exception {
        try (LocalCluster cluster = new LocalCluster(3, tmp, 20);
             QuorumClient client = new QuorumClient(cluster.clientAddresses())) {
            long leader = cluster.awaitLeader(10_000);
            client.put(Bytes.utf8("hello"), Bytes.utf8("world")).get(10, TimeUnit.SECONDS);
            assertEquals("world", Bytes.str(client.get(Bytes.utf8("hello")).get(10, TimeUnit.SECONDS)));
            assertTrue(client.cas(Bytes.utf8("hello"), Bytes.utf8("world"), Bytes.utf8("raft")).get().ok());
            assertFalse(client.cas(Bytes.utf8("hello"), Bytes.utf8("world"), Bytes.utf8("x")).get().ok());
            for (int i = 0; i < 100; i++) client.put(Bytes.utf8("key" + i), Bytes.utf8("v" + i)).get(10, TimeUnit.SECONDS);
            assertEquals(100, client.scan(Bytes.utf8("key"), Bytes.utf8("kez"), 1000).get().size());

            cluster.stop(leader); // kill the leader
            long next = cluster.awaitLeader(10_000);
            assertNotEquals(leader, next);
            assertEquals("raft", Bytes.str(client.get(Bytes.utf8("hello")).get(10, TimeUnit.SECONDS)));
            client.put(Bytes.utf8("after"), Bytes.utf8("failover")).get(10, TimeUnit.SECONDS);

            cluster.start(leader); // the old leader rejoins and catches up from disk + log
            client.put(Bytes.utf8("rejoined"), Bytes.utf8("yes")).get(10, TimeUnit.SECONDS);
            assertTrue(cluster.server(leader).status().contains("\"role\":\"FOLLOWER\""));
            assertTrue(cluster.server(next).prometheus().contains("quorum_requests_total"));
        }
    }

    /** Concurrent clients while the leader is killed and restarted; the history must linearize. */
    @Test
    void historyIsLinearizableAcrossLeaderFailures(@TempDir Path tmp) throws Exception {
        History history = new History();
        long t0 = System.nanoTime();
        try (LocalCluster cluster = new LocalCluster(3, tmp, 20)) {
            cluster.awaitLeader(10_000);
            AtomicBoolean stop = new AtomicBoolean();
            List<Thread> workers = new ArrayList<>();
            for (int c = 0; c < 6; c++) {
                int clientId = c;
                workers.add(Thread.ofVirtual().start(() -> {
                    Random r = new Random(clientId);
                    long counter = 0;
                    try (QuorumClient client = new QuorumClient(cluster.clientAddresses(), 3_000)) {
                        while (!stop.get()) {
                            String key = "k" + r.nextInt(3);
                            if (r.nextBoolean()) {
                                History.Pending p = history.invoke(clientId, Op.Kind.GET, key, null, null, System.nanoTime() - t0);
                                try {
                                    byte[] v = client.get(Bytes.utf8(key)).get();
                                    p.ok(System.nanoTime() - t0, v == null ? null : Bytes.getLongBE(v, 0), false);
                                } catch (Exception e) {
                                    p.fail();
                                }
                            } else {
                                long v = (clientId + 1) * 1_000_000L + ++counter;
                                History.Pending p = history.invoke(clientId, Op.Kind.PUT, key, v, null, System.nanoTime() - t0);
                                try {
                                    client.put(Bytes.utf8(key), Bytes.longBE(v)).get();
                                    p.ok(System.nanoTime() - t0, null, false);
                                } catch (Exception e) {
                                    p.info(System.nanoTime() - t0);
                                }
                            }
                        }
                    }
                }));
            }
            for (int round = 0; round < 3; round++) {
                Thread.sleep(700);
                long leader = cluster.awaitLeader(10_000);
                cluster.stop(leader);
                Thread.sleep(500);
                cluster.start(leader);
            }
            Thread.sleep(700);
            stop.set(true);
            for (Thread w : workers) w.join();
        }
        List<Op> ops = history.ops();
        assertTrue(ops.size() > 100, "too few operations: " + ops.size());
        LinearizabilityChecker.Result r = new LinearizabilityChecker().check(ops);
        assertEquals(LinearizabilityChecker.Verdict.OK, r.verdict(), "history of " + ops.size() + " ops");
    }
}
