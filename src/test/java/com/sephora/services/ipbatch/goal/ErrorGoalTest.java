package com.sephora.services.ipbatch.goal;

import com.sephora.services.ismsearchpoc.ipbatch.goal.ErrorGoal;
import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.ipbatch.goal.GoalStatus;
import com.sephora.services.ismsearchpoc.ipbatch.goal.Severity;
import com.sephora.services.ismsearchpoc.ipbatch.metrics.MetricsSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ErrorGoal evaluation logic
 */
class ErrorGoalTest {
    
    @Test
    void shouldBeMetWhenNoErrors() {
        ErrorGoal goal = new ErrorGoal(
            0.05,    // 5% max error rate per file
            10000,   // max total errors
            Set.of("DB_CONNECTION_LOST", "FILE_CORRUPTION")
        );
        
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(0)
            .recordsProcessed(10000)
            .failedFiles(0)
            .criticalErrorTypes(Set.of())
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.MET, eval.getStatus());
        assertEquals(Severity.HIGH, eval.getSeverity());
    }
    
    @Test
    void shouldBeMetWhenLowErrorRate() {
        ErrorGoal goal = new ErrorGoal(0.05, 10000, Set.of("DB_CONNECTION_LOST"));
        
        // 200 errors out of 100,000 records = 0.2% error rate → MET
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(200)
            .recordsProcessed(100000)
            .failedFiles(2)
            .criticalErrorTypes(Set.of())
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.MET, eval.getStatus());
    }
    
    @Test
    void shouldBeAtRiskWhenApproachingErrorThreshold() {
        ErrorGoal goal = new ErrorGoal(0.05, 10000, Set.of("DB_CONNECTION_LOST"));
        
        // 7,500 errors (75% of 10,000 limit) → AT_RISK
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(7500)
            .recordsProcessed(100000)
            .failedFiles(10)
            .criticalErrorTypes(Set.of())
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.AT_RISK, eval.getStatus());
        
        // Check metrics
        Map<String, Object> goalMetrics = eval.getMetrics();
        assertEquals(7500, goalMetrics.get("totalErrors"));
        assertEquals(2500, goalMetrics.get("errorBudgetRemaining"));
        assertEquals(10, goalMetrics.get("failedFiles"));
    }
    
    @Test
    void shouldBeViolatedWhenExceedingErrorThreshold() {
        ErrorGoal goal = new ErrorGoal(0.05, 10000, Set.of("DB_CONNECTION_LOST"));
        
        // 11,000 errors (exceeded 10,000 limit) → VIOLATED
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(11000)
            .recordsProcessed(100000)
            .failedFiles(20)
            .criticalErrorTypes(Set.of())
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.VIOLATED, eval.getStatus());
    }
    
    @Test
    void shouldBeViolatedWhenCriticalErrorDetected() {
        ErrorGoal goal = new ErrorGoal(
            0.05, 
            10000, 
            Set.of("DB_CONNECTION_LOST", "FILE_CORRUPTION")
        );
        
        // Only 100 errors but has critical error → VIOLATED
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(100)
            .recordsProcessed(10000)
            .failedFiles(1)
            .criticalErrorTypes(Set.of("DB_CONNECTION_LOST"))
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        
        assertEquals(GoalStatus.VIOLATED, eval.getStatus());
        
        Map<String, Object> goalMetrics = eval.getMetrics();
        assertTrue((Boolean) goalMetrics.get("hasCriticalError"));
    }
    
    @Test
    void shouldCalculateErrorRateCorrectly() {
        ErrorGoal goal = new ErrorGoal(0.05, 10000, Set.of());
        
        // 5,000 errors out of 100,000 records = 5% error rate
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(5000)
            .recordsProcessed(100000)
            .failedFiles(50)
            .criticalErrorTypes(Set.of())
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        Map<String, Object> goalMetrics = eval.getMetrics();
        
        double errorRate = (Double) goalMetrics.get("errorRate");
        assertEquals(0.05, errorRate, 0.001);
    }
    
    @Test
    void shouldNotDivideByZeroWithNoRecords() {
        ErrorGoal goal = new ErrorGoal(0.05, 10000, Set.of());
        
        // 0 records processed
        MetricsSnapshot metrics = MetricsSnapshot.builder()
            .totalErrors(0)
            .recordsProcessed(0)
            .failedFiles(0)
            .criticalErrorTypes(Set.of())
            .build();
        
        GoalEvaluation eval = goal.checkStatus(metrics);
        Map<String, Object> goalMetrics = eval.getMetrics();
        
        // Should not throw exception
        assertNotNull(goalMetrics.get("errorRate"));
    }
}
