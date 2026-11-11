package com.objectstore.controller.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ExecutorConfig {

    /** Virtual-thread executor used to talk to several workers in parallel (e.g. writing both replicas of a chunk). */
    @Bean(destroyMethod = "close")
    public ExecutorService workerIoExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
