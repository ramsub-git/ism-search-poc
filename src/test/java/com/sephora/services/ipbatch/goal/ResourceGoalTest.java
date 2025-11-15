package com.sephora.services.ipbatch.goal;

import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalStatus;
import com.sephora.services.ismsearchpoc.ipbatch.goal.ResourceGoal;
import com.sephora.services.ismsearchpoc.ipbatch.goal.Severity;
import com.sephora.services.ismsearchpoc.ipbatch.metrics.MetricsSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ResourceGoal evaluation logic
 */
class ResourceGoalTest {
    
    @Test
    void shouldBeMetWhenResourcesHealthy() {
        ResourceGoal goal = new ResourceGoal(
            100,  // max DB connections
            0.8,  // max DB utilization (80%)
            0.8   // max heap utilization (80%)
        );
        
        // 50% DB, 60% heap → MET
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .activeDbConnections(50)
            .heapUtilization(0.6)
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.MET, eval.getStatus());
        assertEquals(Severity.HIGH, eval.getSeverity());
    }

    @Test
    void shouldBeAtRiskWhenApproachingLimits() {
        ResourceGoal goal = new ResourceGoal(100, 0.8, 0.8);

        // 75% DB utilization → AT_RISK (between 68% and 80%)
        // 85% of 80% = 68% is the AT_RISK threshold
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .activeDbConnections(75)  // 75% utilization
                .heapUtilization(0.6)
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);

        assertEquals(GoalStatus.AT_RISK, eval.getStatus());

        // Check metrics
        Map<String, Object> goalMetrics = eval.getMetrics();
        assertEquals(75.0, (Double) goalMetrics.get("dbUtilizationPercent"), 0.1);
        assertTrue((Boolean) goalMetrics.get("connectionPressure"));
    }

    @Test
    void shouldBeAtRiskWhenHeapApproachingLimit() {
        ResourceGoal goal = new ResourceGoal(100, 0.8, 0.8);

        // 75% heap utilization → AT_RISK (between 68% and 80%)
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .activeDbConnections(50)
                .heapUtilization(0.75)  // 75% utilization
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);

        assertEquals(GoalStatus.AT_RISK, eval.getStatus());
    }
    
    @Test
    void shouldBeViolatedWhenDbExceedsLimit() {
        ResourceGoal goal = new ResourceGoal(100, 0.8, 0.8);
        
        // 85% DB utilization (85/100 = 85% > 80%) → VIOLATED
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .activeDbConnections(85)
            .heapUtilization(0.6)
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.VIOLATED, eval.getStatus());
    }
    
    @Test
    void shouldBeViolatedWhenHeapExceedsLimit() {
        ResourceGoal goal = new ResourceGoal(100, 0.8, 0.8);
        
        // 85% heap utilization → VIOLATED
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .activeDbConnections(50)
            .heapUtilization(0.85)
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.VIOLATED, eval.getStatus());
    }
    
    @Test
    void shouldViolateWhenEitherResourceExceedsLimit() {
        ResourceGoal goal = new ResourceGoal(100, 0.8, 0.8);
        
        // Both high, but heap exceeds limit
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .activeDbConnections(75)
            .heapUtilization(0.90)
            .build();
        
        assertEquals(GoalStatus.VIOLATED, goal.checkStatus(metrics).getStatus());
        
        // DB exceeds, heap ok
        MetricsSnapshot metrics2 = MetricsSnapshot.builder()
            .activeDbConnections(95)
            .heapUtilization(0.50)
            .build();
        
        assertEquals(GoalStatus.VIOLATED, goal.checkStatus(metrics2).getStatus());
    }

    @Test
    void shouldCalculateCorrectMetrics() {
        ResourceGoal goal = new ResourceGoal(100, 0.8, 0.8);

        // Use 60 connections (60%) to be safely in MET range
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .activeDbConnections(60)  // Changed from 70 to 60
                .heapUtilization(0.65)
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);
        Map<String, Object> goalMetrics = eval.getMetrics();

        assertEquals(60.0, (Double) goalMetrics.get("dbUtilizationPercent"), 0.1);
        assertEquals(60, goalMetrics.get("activeConnections"));
        assertEquals(40, goalMetrics.get("availableConnections"));
        assertEquals(80, goalMetrics.get("safeMaxConnections")); // 100 * 0.8
        assertEquals(65.0, (Double) goalMetrics.get("heapUtilizationPercent"), 0.1);
        assertFalse((Boolean) goalMetrics.get("connectionPressure"));
    }
}
