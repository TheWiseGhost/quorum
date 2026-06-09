package io.quorum.common;

import java.util.zip.CRC32C;

/** CRC32C helpers (hardware accelerated on x86 and ARM by the JDK intrinsic). */
public final class Crc {
    private Crc() {}

    public static int crc32c(byte[] b, int off, int len) {
        CRC32C c = new CRC32C();
        c.update(b, off, len);
        return (int) c.getValue();
    }

    public static int crc32c(byte[] b) {
        return crc32c(b, 0, b.length);
    }

    /** Masked like LevelDB so a CRC stored inside checksummed data doesn't checksum to itself. */
    public static int mask(int crc) {
        return ((crc >>> 15) | (crc << 17)) + 0xa282ead8;
    }

    public static int unmask(int masked) {
        int rot = masked - 0xa282ead8;
        return (rot >>> 17) | (rot << 15);
    }
}
