package io.quorum.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Length-prefixed framing: {@code length:u32 (big-endian) | payload}. */
public final class Frames {
    public static final int MAX_FRAME = 256 << 20;

    private Frames() {}

    public static ByteBuffer frame(byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(4 + payload.length);
        b.putInt(payload.length).put(payload).flip();
        return b;
    }

    public static byte[] read(DataInputStream in) throws IOException {
        int len;
        try {
            len = in.readInt();
        } catch (EOFException e) {
            return null;
        }
        if (len < 0 || len > MAX_FRAME) throw new IOException("bad frame length " + len);
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    public static void write(DataOutputStream out, byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
    }
}
