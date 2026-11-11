package com.objectstore.controller.replication;

import java.time.Instant;
import java.util.List;

/** Result of an integrity scan: every UP worker's stored chunks re-hashed and compared with the metadata. */
public record ScrubReport(Instant startedAt, long durationMs, int workersScanned, int chunksVerified,
                          int corruptedFound, int missingFound, int repaired, int dropped, int unrepairable,
                          int orphansRemoved, int staleCopiesRemoved, int adopted, List<WorkerResult> workers) {

    public record WorkerResult(String workerId, boolean reachable, int verified, int corrupted, int missing,
                               int repaired, int dropped, int unrepairable, int orphansRemoved,
                               int staleCopiesRemoved, int adopted) {

        public boolean foundProblems() {
            return corrupted + missing + orphansRemoved + staleCopiesRemoved + adopted > 0;
        }
    }
}
