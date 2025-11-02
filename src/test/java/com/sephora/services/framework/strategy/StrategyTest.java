package com.sephora.services.framework.strategy;

import com.sephora.services.ismsearchpoc.framework.goal.*;
import com.sephora.services.ismsearchpoc.framework.strategy.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for strategy implementations
 */
class StrategyTest {
    
    // ============================================
    // NoOpStrategy Tests
    // ============================================
    
    @Test
    void noOpStrategy_shouldAlwaysReturnNoChange() {
        NoOpStrategy strategy = new NoOpStrategy();
        
        // Create a violated goal evaluation
        Goal goal = mock(Goal.class);
        when(goal.getName()).thenReturn("TestGoal");
        
        GoalEvaluation violatedEval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(new HashMap<>())
            .severity(Severity.CRITICAL)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(violatedEval);
        
        assertTrue(adjustment.isNoChange());
        assertEquals(0, adjustment.getWorkItemConcurrencyDelta());
        assertEquals(0, adjustment.getProcessingConcurrencyDelta());
    }
    
    @Test
    void noOpStrategy_shouldWorkForAnyGoalStatus() {
        NoOpStrategy strategy = new NoOpStrategy();
        Goal goal = mock(Goal.class);
        
        for (GoalStatus status : GoalStatus.values()) {
            GoalEvaluation eval = GoalEvaluation.builder()
                .goal(goal)
                .status(status)
                .metrics(new HashMap<>())
                .severity(Severity.MEDIUM)
                .build();
            
            DialAdjustment adjustment = strategy.recommendAdjustment(eval);
            assertTrue(adjustment.isNoChange());
        }
    }
    
    // ============================================
    // PerformanceStrategy Tests
    // ============================================
    
    @Test
    void performanceStrategy_shouldIncreaseWhenViolated() {
        PerformanceStrategy strategy = new PerformanceStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("rateGap", 10.0);  // Behind by 10 files/min
        metrics.put("percentComplete", 30.0);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metrics)
            .severity(Severity.CRITICAL)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        assertTrue(adjustment.getWorkItemConcurrencyDelta() > 0);
        assertTrue(adjustment.getProcessingConcurrencyDelta() > 0);
        assertTrue(adjustment.getReason().contains("behind"));
    }
    
    @Test
    void performanceStrategy_shouldIncreaseWhenAtRisk() {
        PerformanceStrategy strategy = new PerformanceStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("rateGap", 5.0);
        metrics.put("percentComplete", 50.0);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.AT_RISK)
            .metrics(metrics)
            .severity(Severity.CRITICAL)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        assertTrue(adjustment.getWorkItemConcurrencyDelta() > 0);
    }
    
    @Test
    void performanceStrategy_shouldNotChangeWhenMet() {
        PerformanceStrategy strategy = new PerformanceStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("rateGap", -2.0);  // Ahead by 2 files/min
        metrics.put("percentComplete", 90.0);  // Late in run
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.MET)
            .metrics(metrics)
            .severity(Severity.CRITICAL)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertTrue(adjustment.isNoChange());
    }
    
    @Test
    void performanceStrategy_shouldIncreaseMoreAggressivelyEarly() {
        PerformanceStrategy strategy = new PerformanceStrategy();
        Goal goal = mock(Goal.class);
        
        // Early in run (20% complete)
        Map<String, Object> metricsEarly = new HashMap<>();
        metricsEarly.put("rateGap", 10.0);
        metricsEarly.put("percentComplete", 20.0);
        
        GoalEvaluation evalEarly = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metricsEarly)
            .severity(Severity.CRITICAL)
            .build();
        
        DialAdjustment adjustmentEarly = strategy.recommendAdjustment(evalEarly);
        
        // Late in run (70% complete)
        Map<String, Object> metricsLate = new HashMap<>();
        metricsLate.put("rateGap", 10.0);
        metricsLate.put("percentComplete", 70.0);
        
        GoalEvaluation evalLate = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metricsLate)
            .severity(Severity.CRITICAL)
            .build();
        
        DialAdjustment adjustmentLate = strategy.recommendAdjustment(evalLate);
        
        // Early should increase more than late
        assertTrue(adjustmentEarly.getWorkItemConcurrencyDelta() >= 
                  adjustmentLate.getWorkItemConcurrencyDelta());
    }
    
    // ============================================
    // ResourceStrategy Tests
    // ============================================
    
    @Test
    void resourceStrategy_shouldDecreaseWhenViolated() {
        ResourceStrategy strategy = new ResourceStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("dbUtilizationPercent", 92.0);  // Exceeded 80% limit
        metrics.put("heapUtilizationPercent", 70.0);
        metrics.put("connectionPressure", true);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        assertTrue(adjustment.getWorkItemConcurrencyDelta() < 0);
        assertTrue(adjustment.getProcessingConcurrencyDelta() < 0);
        assertTrue(adjustment.getReason().contains("Resource"));
    }
    
    @Test
    void resourceStrategy_shouldDecreaseWhenAtRisk() {
        ResourceStrategy strategy = new ResourceStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("dbUtilizationPercent", 87.0);  // 85-90% range
        metrics.put("heapUtilizationPercent", 60.0);
        metrics.put("connectionPressure", true);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.AT_RISK)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        assertTrue(adjustment.getWorkItemConcurrencyDelta() < 0);
    }
    
    @Test
    void resourceStrategy_shouldNotChangeWhenMet() {
        ResourceStrategy strategy = new ResourceStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("dbUtilizationPercent", 50.0);
        metrics.put("heapUtilizationPercent", 60.0);
        metrics.put("connectionPressure", false);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.MET)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertTrue(adjustment.isNoChange());
    }
    
    @Test
    void resourceStrategy_shouldDecreaseMoreWhenCritical() {
        ResourceStrategy strategy = new ResourceStrategy();
        Goal goal = mock(Goal.class);
        
        // Critical situation (96% DB)
        Map<String, Object> metricsCritical = new HashMap<>();
        metricsCritical.put("dbUtilizationPercent", 96.0);
        metricsCritical.put("heapUtilizationPercent", 70.0);
        metricsCritical.put("connectionPressure", true);
        
        GoalEvaluation evalCritical = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metricsCritical)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustmentCritical = strategy.recommendAdjustment(evalCritical);
        
        // Moderate situation (84% DB)
        Map<String, Object> metricsModerate = new HashMap<>();
        metricsModerate.put("dbUtilizationPercent", 84.0);
        metricsModerate.put("heapUtilizationPercent", 70.0);
        metricsModerate.put("connectionPressure", true);
        
        GoalEvaluation evalModerate = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metricsModerate)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustmentModerate = strategy.recommendAdjustment(evalModerate);
        
        // Critical should decrease more
        assertTrue(Math.abs(adjustmentCritical.getWorkItemConcurrencyDelta()) > 
                  Math.abs(adjustmentModerate.getWorkItemConcurrencyDelta()));
    }
    
    // ============================================
    // ErrorStrategy Tests
    // ============================================
    
    @Test
    void errorStrategy_shouldDecreaseWhenViolated() {
        ErrorStrategy strategy = new ErrorStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalErrors", 12000);  // Exceeded 10,000 limit
        metrics.put("errorRate", 0.12);     // 12% error rate
        metrics.put("hasCriticalError", false);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        assertTrue(adjustment.getWorkItemConcurrencyDelta() < 0);
        assertTrue(adjustment.getProcessingConcurrencyDelta() < 0);
    }
    
    @Test
    void errorStrategy_shouldDecreaseSignificantlyForCriticalError() {
        ErrorStrategy strategy = new ErrorStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalErrors", 100);
        metrics.put("errorRate", 0.01);
        metrics.put("hasCriticalError", true);  // Critical error!
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.VIOLATED)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        // Should recommend very large decrease (near-shutdown)
        assertTrue(Math.abs(adjustment.getWorkItemConcurrencyDelta()) >= 10);
        assertTrue(adjustment.getReason().contains("Critical error"));
    }
    
    @Test
    void errorStrategy_shouldDecreaseWhenAtRisk() {
        ErrorStrategy strategy = new ErrorStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalErrors", 7500);  // 75% of limit
        metrics.put("errorRate", 0.075);
        metrics.put("hasCriticalError", false);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.AT_RISK)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertFalse(adjustment.isNoChange());
        assertTrue(adjustment.getWorkItemConcurrencyDelta() < 0);
    }
    
    @Test
    void errorStrategy_shouldNotChangeWhenMet() {
        ErrorStrategy strategy = new ErrorStrategy();
        Goal goal = mock(Goal.class);
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalErrors", 100);
        metrics.put("errorRate", 0.001);
        metrics.put("hasCriticalError", false);
        
        GoalEvaluation eval = GoalEvaluation.builder()
            .goal(goal)
            .status(GoalStatus.MET)
            .metrics(metrics)
            .severity(Severity.HIGH)
            .build();
        
        DialAdjustment adjustment = strategy.recommendAdjustment(eval);
        
        assertTrue(adjustment.isNoChange());
    }
}
