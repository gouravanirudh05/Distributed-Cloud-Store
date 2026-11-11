package com.objectstore.controller.object;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ChunkPlacementServiceTest {

    private final ChunkPlacementService placement = new ChunkPlacementService();
    private final List<String> workers = List.of("worker1", "worker2", "worker3", "worker4");

    @Test
    void replicasOfAChunkGoToDistinctWorkers() {
        for (int rotation = 0; rotation < 8; rotation++) {
            List<String> chosen = placement.choose(workers, Map.of(), 2, rotation);
            assertEquals(2, chosen.size());
            assertEquals(2, new HashSet<>(chosen).size(), "copies must be on different workers");
        }
    }

    @Test
    void chunksOfAnObjectAreSpreadEvenlyAcrossAllWorkers() {
        Map<String, Long> load = new HashMap<>();
        Set<Set<String>> pairs = new HashSet<>();
        for (int chunk = 0; chunk < 8; chunk++) {
            List<String> chosen = placement.choose(workers, load, 2, chunk);
            chosen.forEach(w -> load.merge(w, 1L, Long::sum));
            pairs.add(new HashSet<>(chosen));
        }
        // 8 chunks x 2 copies = 16 copies over 4 workers -> exactly 4 each
        workers.forEach(w -> assertEquals(4L, load.get(w), w));
        assertTrue(pairs.size() > 1, "different chunks should land on different worker pairs");
    }

    @Test
    void leastLoadedWorkersArePreferred() {
        Map<String, Long> load = Map.of("worker1", 10L, "worker2", 0L, "worker3", 5L, "worker4", 0L);
        assertEquals(Set.of("worker2", "worker4"), new HashSet<>(placement.choose(workers, load, 2, 0)));
    }

    @Test
    void fewerCandidatesThanCopiesReturnsWhatIsAvailable() {
        assertEquals(List.of("worker3"), placement.choose(List.of("worker3"), Map.of(), 2, 0));
        assertTrue(placement.choose(List.of(), Map.of(), 2, 0).isEmpty());
    }
}
