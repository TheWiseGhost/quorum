package io.quorum.sim;

import io.quorum.common.PlantedBug;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/** The same seed must reproduce the exact same execution, event for event. */
class DeterminismTest {
    @ParameterizedTest
    @ValueSource(longs = {7, 42, 1234})
    void sameSeedSameTrace(long seed) {
        SimConfig cfg = SimConfig.swarm(seed, PlantedBug.NONE, 4_000);
        Simulation.Result a = Simulation.run(seed, cfg, false);
        Simulation.Result b = Simulation.run(seed, cfg, false);
        assertEquals(a.traceHash(), b.traceHash(), "trace hash differs between identical runs");
        assertEquals(a.events(), b.events());
        assertEquals(a.ops(), b.ops());
        assertNotEquals(a.traceHash(), Simulation.run(seed + 1, SimConfig.swarm(seed + 1, PlantedBug.NONE, 4_000), false).traceHash());
    }
}
