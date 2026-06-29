package io.quorum.storage;

/** File naming inside a database directory. */
final class FileNames {
    private FileNames() {}

    static final String CURRENT = "CURRENT";

    static String table(long number) {
        return String.format("%06d.sst", number);
    }

    static String log(long number) {
        return String.format("%06d.log", number);
    }

    static String manifest(long number) {
        return String.format("MANIFEST-%06d", number);
    }

    enum Kind { TABLE, LOG, MANIFEST, CURRENT, TEMP, OTHER }

    record Parsed(Kind kind, long number) {}

    static Parsed parse(String name) {
        try {
            if (name.equals(CURRENT)) return new Parsed(Kind.CURRENT, 0);
            if (name.endsWith(".tmp")) return new Parsed(Kind.TEMP, 0);
            if (name.startsWith("MANIFEST-")) return new Parsed(Kind.MANIFEST, Long.parseLong(name.substring(9)));
            if (name.endsWith(".sst")) return new Parsed(Kind.TABLE, Long.parseLong(name.substring(0, name.length() - 4)));
            if (name.endsWith(".log")) return new Parsed(Kind.LOG, Long.parseLong(name.substring(0, name.length() - 4)));
        } catch (NumberFormatException e) {
            // fall through
        }
        return new Parsed(Kind.OTHER, 0);
    }
}
