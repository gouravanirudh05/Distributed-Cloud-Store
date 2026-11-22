package com.objectstore.controller.cluster;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** The storage cluster's identity (cluster_info.cluster_id), created once with the metadata schema. */
@Component
public class ClusterIdentity {

    private final JdbcTemplate jdbc;
    private volatile String clusterId;

    public ClusterIdentity(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public String clusterId() {
        String id = clusterId;
        if (id == null) {
            id = jdbc.queryForObject("SELECT cluster_id FROM cluster_info WHERE id = 1", String.class);
            clusterId = id;
        }
        return id;
    }
}
