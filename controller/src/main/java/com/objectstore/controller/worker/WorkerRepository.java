package com.objectstore.controller.worker;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface WorkerRepository extends JpaRepository<WorkerNode, String> {
    List<WorkerNode> findByStatus(WorkerStatus status);
}
