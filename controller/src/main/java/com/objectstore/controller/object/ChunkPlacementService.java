package com.objectstore.controller.object;

import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Chooses which workers receive a chunk's replicas: workers holding the fewest replicas first, and among
 * equally loaded workers a rotating order, so consecutive chunks of an object spread across all workers.
 * The caller guarantees the candidate list only contains distinct, live workers.
 */
@Component
public class ChunkPlacementService {

    public List<String> rank(Collection<String> candidates, Map<String, Long> load, int rotation) {
        List<String> sorted = new ArrayList<>(new TreeSet<>(candidates));
        int n = sorted.size();
        if (n == 0) return List.of();
        int shift = Math.floorMod(rotation, n);
        List<String> rotated = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rotated.add(sorted.get((i + shift) % n));
        }
        rotated.sort(Comparator.comparingLong(id -> load.getOrDefault(id, 0L))); // stable: keeps rotation order on ties
        return rotated;
    }

    public List<String> choose(Collection<String> candidates, Map<String, Long> load, int count, int rotation) {
        List<String> ranked = rank(candidates, load, rotation);
        return List.copyOf(ranked.subList(0, Math.min(count, ranked.size())));
    }
}
