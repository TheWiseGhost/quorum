package io.quorum.sim;

import io.quorum.common.PlantedBug;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** A quick sweep for every build; the large sweeps run via {@code simSweep} in CI. */
class SimulationSmokeTest {
    @Test
    void fiftySeedsPass() {
        for (long seed = 1; seed <= 50; seed++) {
            Simulation.Result r = Simulation.run(seed, SimConfig.swarm(seed, PlantedBug.NONE, 5_000), false);
            assertTrue(r.ok(), "seed " + seed + ": " + r.failure());
            assertTrue(r.ops() > 0, "seed " + seed + " made no progress");
        }
    }

    @Test
    void plantedDedupBugIsCaughtWithinAFewSeeds() {
        for (long seed = 1; seed <= 40; seed++) {
            Simulation.Result r = Simulation.run(seed, SimConfig.swarm(seed, PlantedBug.of(PlantedBug.NO_CLIENT_DEDUP), 10_000), false);
            if (!r.ok()) return;
        }
        fail("40 seeds never caught a retried CAS being applied twice");
    }
}
