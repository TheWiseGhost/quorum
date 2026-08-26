package io.quorum.sim;

import io.quorum.common.PlantedBug;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Command-line entry point.
 * <pre>
 *   SimRunner --seeds 1000 [--start 1] [--duration 20000] [--bug FIGURE8_COMMIT]
 *   SimRunner --seed 42 --trace          replay one seed and print its event trace
 *   SimRunner --seed 42 --determinism    run twice and compare trace hashes
 * </pre>
 */
public final class SimRunner {
    public static void main(String[] args) throws Exception {
        long start = 1, seeds = 100, duration = 15_000;
        Long single = null;
        boolean trace = false, determinism = false;
        Set<PlantedBug> bugs = EnumSet.noneOf(PlantedBug.class);
        int threads = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--seeds" -> seeds = Long.parseLong(args[++i]);
                case "--start" -> start = Long.parseLong(args[++i]);
                case "--seed" -> single = Long.parseLong(args[++i]);
                case "--duration" -> duration = Long.parseLong(args[++i]);
                case "--bug" -> bugs.add(PlantedBug.valueOf(args[++i]));
                case "--threads" -> threads = Integer.parseInt(args[++i]);
                case "--trace" -> trace = true;
                case "--determinism" -> determinism = true;
                default -> throw new IllegalArgumentException("unknown arg " + args[i]);
            }
        }
        Set<PlantedBug> bugSet = Set.copyOf(bugs);
        if (single != null) {
            SimConfig cfg = SimConfig.swarm(single, bugSet, duration);
            Simulation.Result r = Simulation.run(single, cfg, trace);
            if (trace) r.trace().forEach(System.out::println);
            print(r);
            if (determinism) {
                Simulation.Result again = Simulation.run(single, cfg, false);
                boolean same = again.traceHash() == r.traceHash() && again.events() == r.events();
                System.out.printf("determinism: run1=%016x run2=%016x %s%n", r.traceHash(), again.traceHash(),
                        same ? "IDENTICAL" : "DIFFERENT");
                if (!same) System.exit(2);
            }
            System.exit(r.ok() ? 0 : 1);
        }

        long t0 = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Simulation.Result>> futures = new ArrayList<>();
        for (long s = start; s < start + seeds; s++) {
            long seed = s;
            long d = duration;
            futures.add(pool.submit(() -> Simulation.run(seed, SimConfig.swarm(seed, bugSet, d), false)));
        }
        int failures = 0;
        long ops = 0, events = 0, simMs = 0, leaderChanges = 0, faults = 0, snaps = 0, member = 0, unknown = 0;
        for (Future<Simulation.Result> f : futures) {
            Simulation.Result r = f.get();
            ops += r.ops();
            events += r.events();
            simMs += r.simMillis();
            leaderChanges += r.leaderChanges();
            faults += r.faults();
            snaps += r.snapshots();
            member += r.membershipChanges();
            if (r.linearizable() == io.quorum.check.LinearizabilityChecker.Verdict.UNKNOWN) unknown++;
            if (!r.ok()) {
                failures++;
                print(r);
            }
        }
        pool.shutdown();
        double wall = (System.nanoTime() - t0) / 1e9;
        System.out.printf("%n%d seeds [%d..%d], %d failed%s%n", seeds, start, start + seeds - 1, failures,
                bugSet.isEmpty() ? "" : "  (planted: " + bugSet + ")");
        System.out.printf("  %,d client ops checked, %,d leader elections, %,d faults injected, %,d membership changes, %,d snapshots%n",
                ops, leaderChanges, faults, member, snaps);
        System.out.printf("  %,d events, %.1f simulated minutes in %.1fs wall (%.0fx real time)%s%n", events,
                simMs / 60_000.0, wall, simMs / 1000.0 / wall, unknown > 0 ? ", " + unknown + " histories too large to decide" : "");
        System.exit(failures == 0 ? 0 : 1);
    }

    static void print(Simulation.Result r) {
        System.out.printf("seed %d: %s  [%s]%n", r.seed(), r.ok() ? "OK" : "FAIL", r.config());
        System.out.printf("  ops=%d (indeterminate %d) elections=%d faults=%d membership=%d snapshots=%d msgs=%d dropped=%d events=%d hash=%016x%n",
                r.ops(), r.infoOps(), r.leaderChanges(), r.faults(), r.membershipChanges(), r.snapshots(), r.messages(),
                r.dropped(), r.events(), r.traceHash());
        if (!r.ok()) {
            System.out.println("  " + r.failure().replace("\n", "\n    "));
            Path p = Simulation.writeFailureArtifact(r);
            if (p != null) System.out.println("  history: " + p.toAbsolutePath());
            System.out.println("  replay:  ./gradlew :quorum-sim:simSweep -PsimArgs=\"--seed " + r.seed() + " --trace\"");
        }
    }
}
