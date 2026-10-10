package io.quorum.sim;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Enforces the one architectural rule: core modules never touch real time, threads, sockets,
 * files or unseeded randomness directly. Everything goes through the injected interfaces, which
 * is what makes deterministic simulation possible.
 */
class DeterministicCoreRuleTest {
    private static final List<String> CORE = List.of("quorum-raft", "quorum-storage", "quorum-kv");
    private static final Pattern BANNED = Pattern.compile(
            "System\\.nanoTime|System\\.currentTimeMillis|Instant\\.now|LocalDateTime\\.now|Math\\.random"
                    + "|ThreadLocalRandom|new Random\\(|new Thread\\(|Thread\\.ofPlatform|Thread\\.ofVirtual|Thread\\.sleep"
                    + "|Executors\\.|java\\.io\\.File[^a-zA-Z]|java\\.nio\\.channels|java\\.nio\\.file\\.Files|java\\.net\\."
                    + "|parallelStream|CompletableFuture\\.\\w+Async|identityHashCode");

    @Test
    void coreModulesUseOnlyInjectedIo() throws IOException {
        Path root = Path.of("").toAbsolutePath().getParent();
        List<String> violations = new ArrayList<>();
        int scanned = 0;
        for (String module : CORE) {
            Path src = root.resolve(module).resolve("src/main/java");
            try (Stream<Path> files = Files.walk(src)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    scanned++;
                    List<String> lines = Files.readAllLines(f);
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i).strip();
                        if (line.startsWith("*") || line.startsWith("//") || line.startsWith("/*")) continue;
                        if (BANNED.matcher(line).find()) violations.add(root.relativize(f) + ":" + (i + 1) + "  " + line);
                    }
                }
            }
        }
        assertTrue(scanned > 30, "expected to scan core sources, found " + scanned);
        assertTrue(violations.isEmpty(), "non-deterministic API used in core code:\n" + String.join("\n", violations));
    }
}
