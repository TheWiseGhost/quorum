package io.quorum.sim;

import io.quorum.common.PlantedBug;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Mutation testing for the test harness: enables each {@link PlantedBug} in turn and searches
 * seeds until the simulator reports a failure. A bug that survives every seed means the checks
 * have a blind spot.
 */
public final class PlantedBugSuite {
    public record Outcome(PlantedBug bug, boolean caught, long seed, long seedsTried, String reason) {}

    public static void main(String[] args) throws Exception {
        int maxSeeds = 300;
        long duration = 10_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--max-seeds" -> maxSeeds = Integer.parseInt(args[++i]);
                case "--duration" -> duration = Long.parseLong(args[++i]);
                default -> throw new IllegalArgumentException(args[i]);
            }
        }
        List<Outcome> outcomes = new ArrayList<>();
        for (PlantedBug bug : PlantedBug.values()) outcomes.add(hunt(bug, maxSeeds, duration));
        System.out.printf("%n%-22s %-8s %-8s %-6s %s%n", "planted bug", "caught", "seed", "tries", "first failure");
        boolean all = true;
        for (Outcome o : outcomes) {
            all &= o.caught();
            String reason = o.reason() == null ? "" : o.reason().split("\n")[0];
            if (reason.length() > 110) reason = reason.substring(0, 110) + "…";
            System.out.printf("%-22s %-8s %-8s %-6d %s%n", o.bug(), o.caught() ? "yes" : "NO", o.caught() ? o.seed() : "-",
                    o.seedsTried(), reason);
        }
        System.exit(all ? 0 : 1);
    }

    /** Runs seeds in parallel batches until one fails. */
    public static Outcome hunt(PlantedBug bug, int maxSeeds, long duration) throws Exception {
        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (long base = 1; base <= maxSeeds; base += threads) {
                List<Future<Simulation.Result>> batch = new ArrayList<>();
                for (long s = base; s < base + threads && s <= maxSeeds; s++) {
                    long seed = s;
                    batch.add(pool.submit(() -> Simulation.run(seed, SimConfig.swarm(seed, Set.of(bug), duration), false)));
                }
                Simulation.Result first = null;
                for (Future<Simulation.Result> f : batch) {
                    Simulation.Result r = f.get();
                    if (!r.ok() && (first == null || r.seed() < first.seed())) first = r;
                }
                if (first != null) return new Outcome(bug, true, first.seed(), first.seed(), first.failure());
            }
            return new Outcome(bug, false, -1, maxSeeds, null);
        } finally {
            pool.shutdownNow();
        }
    }
}
