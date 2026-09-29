package io.quorum.bench;

import io.quorum.client.QuorumClient;
import io.quorum.common.ByteReader;
import io.quorum.common.ByteWriter;
import io.quorum.common.Bytes;
import io.quorum.kv.KvStateMachine;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Binding for the official YCSB harness. Records are stored as one value per key with the field
 * map encoded inline. Configure with {@code -p quorum.servers=h:p,h:p,h:p}.
 *
 * <pre>
 * ./bin/ycsb run basic -P workloads/workloada -cp quorum-bench.jar \
 *     -db io.quorum.bench.QuorumYcsbClient -p quorum.servers=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
 * </pre>
 */
public final class QuorumYcsbClient extends DB {
    private QuorumClient client;

    @Override
    public void init() throws DBException {
        String servers = getProperties().getProperty("quorum.servers", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003");
        List<InetSocketAddress> addrs = new ArrayList<>();
        for (String s : servers.split(",")) {
            int c = s.lastIndexOf(':');
            addrs.add(new InetSocketAddress(s.substring(0, c), Integer.parseInt(s.substring(c + 1))));
        }
        client = new QuorumClient(addrs);
    }

    @Override
    public void cleanup() {
        client.close();
    }

    private static byte[] key(String table, String key) {
        return Bytes.utf8(table + "/" + key);
    }

    private static byte[] encode(Map<String, ByteIterator> values) {
        ByteWriter w = new ByteWriter(256).putVarInt(values.size());
        values.forEach((k, v) -> w.putString(k).putBytes(v.toArray()));
        return w.toByteArray();
    }

    private static void decode(byte[] b, Set<String> fields, Map<String, ByteIterator> out) {
        ByteReader r = new ByteReader(b);
        for (int n = r.getVarInt(); n > 0; n--) {
            String f = r.getString();
            byte[] v = r.getBytes();
            if (fields == null || fields.contains(f)) out.put(f, new ByteArrayByteIterator(v));
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            byte[] v = client.get(key(table, key)).get();
            if (v == null) return Status.NOT_FOUND;
            decode(v, fields, result);
            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String table, String startkey, int count, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        try {
            for (KvStateMachine.Row row : client.scan(key(table, startkey), Bytes.prefixEnd(Bytes.utf8(table + "/")), count).get()) {
                HashMap<String, ByteIterator> m = new HashMap<>();
                decode(row.value(), fields, m);
                result.add(m);
            }
            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        try {
            // Read-merge-CAS so partial updates never clobber concurrent writers.
            byte[] k = key(table, key);
            for (int attempt = 0; attempt < 16; attempt++) {
                byte[] cur = client.get(k).get();
                Map<String, ByteIterator> merged = new HashMap<>();
                if (cur != null) decode(cur, null, merged);
                merged.putAll(values);
                if (client.cas(k, cur, encode(merged)).get().ok()) return Status.OK;
            }
            return Status.ERROR;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            client.put(key(table, key), encode(values)).get();
            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status delete(String table, String key) {
        try {
            client.delete(key(table, key)).get();
            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }
}
