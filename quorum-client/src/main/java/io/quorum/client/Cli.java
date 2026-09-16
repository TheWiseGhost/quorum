package io.quorum.client;

import io.quorum.common.Bytes;
import io.quorum.kv.KvStateMachine;
import io.quorum.kv.Request;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <pre>
 * quorum [--servers h:p,h:p,...] put KEY VALUE | get KEY | delete KEY | cas KEY OLD NEW
 *        | scan [FROM [TO]] | status | add-learner ID | promote ID | remove ID | transfer ID
 * </pre>
 * Server order defines node ids (first = node 1). Defaults to localhost:8001-8003.
 */
public final class Cli {
    public static void main(String[] argv) throws Exception {
        List<String> args = new ArrayList<>(Arrays.asList(argv));
        String servers = "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003";
        if (args.size() >= 2 && args.getFirst().equals("--servers")) {
            servers = args.get(1);
            args = args.subList(2, args.size());
        }
        if (args.isEmpty()) {
            System.err.println("usage: quorum [--servers h:p,...] put|get|delete|cas|scan|status|add-learner|promote|remove|transfer ...");
            System.exit(2);
        }
        List<InetSocketAddress> addrs = new ArrayList<>();
        for (String s : servers.split(",")) {
            int c = s.lastIndexOf(':');
            addrs.add(new InetSocketAddress(s.substring(0, c), Integer.parseInt(s.substring(c + 1))));
        }
        try (QuorumClient client = new QuorumClient(addrs)) {
            String cmd = args.getFirst();
            switch (cmd) {
                case "put" -> {
                    client.put(Bytes.utf8(args.get(1)), Bytes.utf8(args.get(2))).get();
                    System.out.println("OK");
                }
                case "get" -> {
                    byte[] v = client.get(Bytes.utf8(args.get(1))).get();
                    System.out.println(v == null ? "(not found)" : Bytes.str(v));
                }
                case "delete" -> {
                    client.delete(Bytes.utf8(args.get(1))).get();
                    System.out.println("OK");
                }
                case "cas" -> {
                    byte[] expected = args.get(2).equals("-") ? null : Bytes.utf8(args.get(2));
                    byte[] next = args.get(3).equals("-") ? null : Bytes.utf8(args.get(3));
                    QuorumClient.CasResult r = client.cas(Bytes.utf8(args.get(1)), expected, next).get();
                    System.out.println(r.ok() ? "OK" : "FAILED (current: " + (r.current() == null ? "(absent)" : Bytes.str(r.current())) + ")");
                }
                case "scan" -> {
                    byte[] from = args.size() > 1 ? Bytes.utf8(args.get(1)) : null;
                    byte[] to = args.size() > 2 ? Bytes.utf8(args.get(2)) : null;
                    for (KvStateMachine.Row row : client.scan(from, to, 1000).get()) {
                        System.out.println(Bytes.str(row.key()) + " = " + Bytes.str(row.value()));
                    }
                }
                case "status" -> {
                    for (int i = 0; i < addrs.size(); i++) {
                        try {
                            System.out.println(client.status(i).get(2, java.util.concurrent.TimeUnit.SECONDS));
                        } catch (Exception e) {
                            System.out.println("{\"server\":\"" + addrs.get(i) + "\",\"error\":\"unreachable\"}");
                        }
                    }
                }
                case "add-learner" -> System.out.println(client.admin(new Request.AddLearner(Long.parseLong(args.get(1)))).get());
                case "promote" -> System.out.println(client.admin(new Request.Promote(Long.parseLong(args.get(1)))).get());
                case "remove" -> System.out.println(client.admin(new Request.Remove(Long.parseLong(args.get(1)))).get());
                case "transfer" -> System.out.println(client.admin(new Request.TransferLeader(Long.parseLong(args.get(1)))).get());
                default -> {
                    System.err.println("unknown command " + cmd);
                    System.exit(2);
                }
            }
        }
    }
}
