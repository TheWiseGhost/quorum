package io.quorum.server;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Process configuration.
 *
 * @param peers     every node's peer (Raft) address, including this one
 * @param bootstrap initial voter ids for a founding member; empty to join an existing cluster
 */
public record ServerConfig(long id, Map<Long, InetSocketAddress> peers, InetSocketAddress clientAddress,
                           int metricsPort, Path dataDir, TreeSet<Long> bootstrap, long tickMs, long snapshotEvery) {

    /**
     * Parses {@code --id 1 --peers 1=host:7001,2=host:7002,3=host:7003 --client host:8001
     * --data ./data/n1 [--bootstrap 1,2,3 | --join] [--metrics 9001] [--tick-ms 50]}.
     */
    public static ServerConfig parse(String[] args) {
        long id = -1;
        Map<Long, InetSocketAddress> peers = new TreeMap<>();
        InetSocketAddress client = null;
        int metrics = 0;
        Path data = null;
        TreeSet<Long> bootstrap = null;
        boolean join = false;
        long tick = 50, snapEvery = 10_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> id = Long.parseLong(args[++i]);
                case "--peers" -> {
                    for (String p : args[++i].split(",")) {
                        String[] kv = p.split("=");
                        peers.put(Long.parseLong(kv[0]), address(kv[1]));
                    }
                }
                case "--client" -> client = address(args[++i]);
                case "--metrics" -> metrics = Integer.parseInt(args[++i]);
                case "--data" -> data = Path.of(args[++i]);
                case "--bootstrap" -> {
                    bootstrap = new TreeSet<>();
                    for (String s : args[++i].split(",")) bootstrap.add(Long.parseLong(s));
                }
                case "--join" -> join = true;
                case "--tick-ms" -> tick = Long.parseLong(args[++i]);
                case "--snapshot-every" -> snapEvery = Long.parseLong(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (id < 0 || peers.isEmpty() || client == null || data == null) {
            throw new IllegalArgumentException("usage: --id N --peers 1=h:p,... --client h:p --data DIR [--bootstrap 1,2,3 | --join]");
        }
        if (bootstrap == null) bootstrap = join ? new TreeSet<>() : new TreeSet<>(peers.keySet());
        return new ServerConfig(id, peers, client, metrics, data, bootstrap, tick, snapEvery);
    }

    static InetSocketAddress address(String hostPort) {
        int c = hostPort.lastIndexOf(':');
        return new InetSocketAddress(hostPort.substring(0, c), Integer.parseInt(hostPort.substring(c + 1)));
    }
}
