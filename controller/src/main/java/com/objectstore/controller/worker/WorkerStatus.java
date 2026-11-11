package com.objectstore.controller.worker;

/**
 * UP         - heartbeating and verified; used for reads and writes.
 * DOWN       - missed heartbeats for longer than store.heartbeat-timeout-ms.
 * RECOVERING - (re)registered or heartbeating again after being DOWN; its stored chunks are verified
 *              against the metadata before it is promoted back to UP.
 */
public enum WorkerStatus { UP, DOWN, RECOVERING }
