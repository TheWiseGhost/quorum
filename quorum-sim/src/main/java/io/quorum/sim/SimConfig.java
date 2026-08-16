package io.quorum.sim;

import io.quorum.common.PlantedBug;
import io.quorum.common.Rng;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * One run's parameters. {@link #swarm} draws a random subset of faults and intensities per seed
 * (swarm testing): uniform fault mixes tend to mask bugs that only appear under a specific mix.
 */
public record SimConfig(
        int initialVoters,
        int maxNodes,
        int clients,
        int keys,
        long durationMs,
        long tickMs,
        boolean crashes,
        boolean partitions,
        boolean pauses,
        boolean membership,
        boolean clockSkew,
        double dropRate,
        double duplicateRate,
        long minLatencyUs,
        long maxLatencyUs,
        long faultIntervalMs,
        long snapshotEvery,
        Set<PlantedBug> bugs) {

    public static SimConfig swarm(long seed, Set<PlantedBug> bugs, long durationMs) {
        Rng r = Rng.seeded(seed ^ 0x5DEECE66DL);
        int voters = r.chance(0.6) ? 3 : 5;
        boolean crashes = r.chance(0.75), partitions = r.chance(0.75), pauses = r.chance(0.4);
        boolean membership = r.chance(0.35), skew = r.chance(0.4);
        if (!crashes && !partitions && !pauses && !membership) partitions = true;
        double drop = r.chance(0.5) ? r.nextDouble() * 0.15 : 0;
        double dup = r.chance(0.3) ? r.nextDouble() * 0.05 : 0;
        long minLat = r.between(100, 2_000);
        long maxLat = minLat + r.between(0, 20_000);
        long interval = r.between(150, 1_500);
        long snapEvery = r.chance(0.5) ? r.between(20, 200) : 1_000_000;
        return new SimConfig(voters, 5, (int) r.between(3, 8), (int) r.between(2, 6), durationMs, 10,
                crashes, partitions, pauses, membership, skew, drop, dup, minLat, maxLat, interval, snapEvery, bugs);
    }

    public String describe() {
        List<String> faults = new ArrayList<>();
        if (crashes) faults.add("crash");
        if (partitions) faults.add("partition");
        if (pauses) faults.add("pause");
        if (membership) faults.add("membership");
        if (clockSkew) faults.add("skew");
        if (dropRate > 0) faults.add(String.format("drop%.0f%%", dropRate * 100));
        if (duplicateRate > 0) faults.add(String.format("dup%.1f%%", duplicateRate * 100));
        return String.format("voters=%d clients=%d keys=%d faults=%s snapEvery=%s", initialVoters, clients, keys,
                String.join(",", faults), snapshotEvery >= 1_000_000 ? "off" : Long.toString(snapshotEvery));
    }
}
