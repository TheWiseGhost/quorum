package io.quorum.common;

/**
 * Seeded randomness. Every component receives its own {@link #split() child} so adding a
 * component never shifts the random stream another component sees.
 */
public interface Rng {
    long nextLong();

    int nextInt(int bound);

    double nextDouble();

    Rng split();

    default boolean chance(double probability) {
        return probability > 0 && nextDouble() < probability;
    }

    /** Uniform in {@code [lo, hi)}. */
    default long between(long lo, long hi) {
        if (hi <= lo) return lo;
        return lo + Math.floorMod(nextLong(), hi - lo);
    }

    static Rng seeded(long seed) {
        return new SeededRng(new java.util.SplittableRandom(seed));
    }

    final class SeededRng implements Rng {
        private final java.util.SplittableRandom r;

        private SeededRng(java.util.SplittableRandom r) {
            this.r = r;
        }

        @Override
        public long nextLong() {
            return r.nextLong();
        }

        @Override
        public int nextInt(int bound) {
            return r.nextInt(bound);
        }

        @Override
        public double nextDouble() {
            return r.nextDouble();
        }

        @Override
        public Rng split() {
            return new SeededRng(r.split());
        }
    }
}
