package com.objectstore.controller.worker;

/** A call to a worker failed (unreachable, timed out, or returned an error status). */
public class WorkerException extends RuntimeException {
    public WorkerException(String message, Throwable cause) {
        super(message, cause);
    }
}
