package com.sephora.services.ismsearchpoc.ipbatch.runtime;

import com.sephora.services.ismsearchpoc.ipbatch.strategy.ResourceSnapshot;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class ResourceMonitor {
    
    private final HikariDataSource dataSource;
    
    public ResourceMonitor(@Qualifier("mysqlDataSource") HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    public ResourceSnapshot snapshot() {
        int maxDbConnections = dataSource.getMaximumPoolSize();
        int activeDbConnections = dataSource.getHikariPoolMXBean().getActiveConnections();
        int availableDbConnections = maxDbConnections - activeDbConnections;
        
        Runtime runtime = Runtime.getRuntime();
        long maxHeap = runtime.maxMemory();
        long usedHeap = runtime.totalMemory() - runtime.freeMemory();
        double availableHeapPercent = 1.0 - ((double) usedHeap / maxHeap);
        
        int availableCpuCores = Runtime.getRuntime().availableProcessors();
        
        return ResourceSnapshot.builder()
            .availableDbConnections(availableDbConnections)
            .availableHeapPercent(availableHeapPercent)
            .availableCpuCores(availableCpuCores)
            .build();
    }
}
