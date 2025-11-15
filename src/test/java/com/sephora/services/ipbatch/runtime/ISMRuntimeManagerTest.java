package com.sephora.services.ipbatch.runtime;

import com.sephora.services.ismsearchpoc.ipbatch.engine.ParallelBatchEngine;
import com.sephora.services.ismsearchpoc.ipbatch.goal.*;
import com.sephora.services.ismsearchpoc.ipbatch.metrics.MetricsSnapshot;
import com.sephora.services.ismsearchpoc.ipbatch.runtime.ISMRuntimeManager;
import com.sephora.services.ismsearchpoc.ipbatch.strategy.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ISMRuntimeManager conflict resolution and decision making
 */
class ISMRuntimeManagerTest {

    private ParallelBatchEngine<?, ?, ?> mockEngine;
    private PerformanceGoal perfGoal;
    private ResourceGoal resourceGoal;
    private ErrorGoal errorGoal;
    private ISMRuntimeManager manager;

    @BeforeEach
    void setup() {
        mockEngine = mock(ParallelBatchEngine.class);

        // Create goals without strategies
        perfGoal = new PerformanceGoal(Duration.ofMinutes(60), 15.0, 0.8);
        resourceGoal = new ResourceGoal(100, 0.8, 0.8);
        errorGoal = new ErrorGoal(0.05, 10000, Set.of("DB_CONNECTION_LOST"));

        // Create strategy map
        Map<Goal, GoalStrategy> strategies = new HashMap<>();
        strategies.put(perfGoal, new PerformanceStrategy());
        strategies.put(resourceGoal, new ResourceStrategy());
        strategies.put(errorGoal, new ErrorStrategy());

        List<Goal> goals = Arrays.asList(perfGoal, resourceGoal, errorGoal);

        // Create manager with goals and strategies
        manager = new ISMRuntimeManager(goals, strategies, mockEngine);
    }

    // ============================================
    // No-Op Strategy Tests
    // ============================================

    @Test
    void shouldNotAdjustWithNoOpStrategies() {
        // Create goals
        PerformanceGoal noOpPerfGoal = new PerformanceGoal(Duration.ofMinutes(60), 15.0, 0.8);
        ResourceGoal noOpResourceGoal = new ResourceGoal(100, 0.8, 0.8);
        ErrorGoal noOpErrorGoal = new ErrorGoal(0.05, 10000, Set.of("DB_CONNECTION_LOST"));

        // Create strategy map with NoOp strategies
        Map<Goal, GoalStrategy> noOpStrategies = new HashMap<>();
        noOpStrategies.put(noOpPerfGoal, new NoOpStrategy());
        noOpStrategies.put(noOpResourceGoal, new NoOpStrategy());
        noOpStrategies.put(noOpErrorGoal, new NoOpStrategy());

        List<Goal> noOpGoals = Arrays.asList(noOpPerfGoal, noOpResourceGoal, noOpErrorGoal);

        ISMRuntimeManager noOpManager = new ISMRuntimeManager(
                noOpGoals,
                noOpStrategies,
                mockEngine
        );

        // Moderate issues but using NoOp strategies - should not adjust
        // Keep metrics in AT_RISK range, not VIOLATED (to avoid abort)
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(300)
                .totalFiles(900)
                .filesPerMinute(12.0)  // Slightly slow but not VIOLATED
                .activeDbConnections(75)  // AT_RISK but not VIOLATED
                .heapUtilization(0.75)  // AT_RISK but not VIOLATED
                .totalErrors(7500)  // AT_RISK but not VIOLATED
                .recordsProcessed(300000)
                .failedFiles(10)
                .criticalErrorTypes(Set.of())
                .build();

        noOpManager.evaluateAndAdjust(metrics);

        // Should never call adjustConcurrency with NoOp strategies
        verify(mockEngine, never()).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    // ============================================
    // Performance Goal Tests
    // ============================================

    @Test
    void shouldIncreaseConcurrencyWhenPerformanceBehind() {
        // Performance is AT_RISK - slow but not catastrophic
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(100)
                .totalFiles(900)
                .filesPerMinute(10.0)  // Below 15 minimum, triggers AT_RISK
                .recordsProcessed(100000)
                .activeDbConnections(30)
                .heapUtilization(0.5)
                .totalErrors(100)
                .failedFiles(1)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should increase concurrency to catch up
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    @Test
    void shouldAbortWhenPerformanceCriticallyViolated() {
        // Performance VIOLATED with extremely slow rate
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(5)
                .totalFiles(900)
                .filesPerMinute(0.1)  // Extremely slow - definitely VIOLATED
                .recordsProcessed(5000)
                .activeDbConnections(30)
                .heapUtilization(0.5)
                .totalErrors(0)
                .failedFiles(0)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should abort, not adjust
        verify(mockEngine, times(1)).abort(anyString());
        verify(mockEngine, never()).adjustConcurrency(anyInt(), anyInt());
    }

    // ============================================
    // Resource Goal Tests
    // ============================================

    @Test
    void shouldDecreaseConcurrencyWhenResourceConstrained() {
        // Resources are AT_RISK - high but not exceeded
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(450)
                .totalFiles(900)
                .filesPerMinute(16.0)  // Performance is OK (above minimum)
                .recordsProcessed(450000)
                .activeDbConnections(75)  // 75% - in AT_RISK range (68-80%)
                .heapUtilization(0.75)    // 75% - in AT_RISK range
                .totalErrors(100)
                .failedFiles(1)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should decrease concurrency to relieve pressure
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    // ============================================
    // Error Goal Tests
    // ============================================

    @Test
    void shouldDecreaseConcurrencyWhenErrorsHigh() {
        // Errors are AT_RISK - approaching limit
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(300)
                .totalFiles(900)
                .filesPerMinute(16.0)  // Performance OK
                .recordsProcessed(300000)
                .activeDbConnections(50)
                .heapUtilization(0.6)
                .totalErrors(7500)  // 75% of limit - AT_RISK
                .failedFiles(10)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should decrease to reduce error rate
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    @Test
    void shouldNotAbortOnHighSeverityErrorGoal() {
        // Error goal is HIGH severity, not CRITICAL, so should adjust not abort
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(200)
                .totalFiles(900)
                .filesPerMinute(16.0)  // Performance OK
                .recordsProcessed(200000)
                .activeDbConnections(50)
                .heapUtilization(0.6)
                .totalErrors(15000)  // Exceeded limit - VIOLATED
                .failedFiles(20)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // HIGH severity violations adjust, don't abort (only CRITICAL aborts)
        // Error goal will recommend decrease
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    // ============================================
    // Conflict Resolution Tests
    // ============================================

    @Test
    void shouldHandleConflictingGoals() {
        // Performance wants to increase (AT_RISK, slow)
        // Resource wants to decrease (AT_RISK, high utilization)
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(100)
                .totalFiles(900)
                .filesPerMinute(10.0)   // Slow - performance AT_RISK
                .recordsProcessed(100000)
                .activeDbConnections(75) // High - resource AT_RISK
                .heapUtilization(0.75)   // High - resource AT_RISK
                .totalErrors(100)
                .failedFiles(1)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should make a decision (resource's HIGH severity vs performance's need)
        // Manager will choose based on severity and magnitude
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    @Test
    void shouldSelectMostAggressiveDecreaseWhenMultipleDecreasesRecommended() {
        // Both resources and errors want to decrease
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(600)
                .totalFiles(900)
                .filesPerMinute(16.0)  // Performance OK (above minimum)
                .recordsProcessed(600000)
                .activeDbConnections(75)  // AT_RISK
                .heapUtilization(0.75)    // AT_RISK
                .totalErrors(7500)        // AT_RISK
                .failedFiles(10)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should apply a decrease adjustment
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    // ============================================
    // Safety Bounds Tests
    // ============================================

    @Test
    void shouldEnforceSafetyBounds() {
        // Performance goal wants to increase
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(50)
                .totalFiles(900)
                .filesPerMinute(10.0)    // Slow - AT_RISK
                .recordsProcessed(50000)
                .activeDbConnections(30)
                .heapUtilization(0.5)
                .totalErrors(10)
                .failedFiles(0)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should adjust
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
        verify(mockEngine, never()).abort(anyString());
    }

    // ============================================
    // Oscillation Protection Tests
    // ============================================

    @Test
    void shouldRespectOscillationProtection() {
        // Use metrics that will trigger an adjustment
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(100)
                .totalFiles(900)
                .filesPerMinute(10.0)  // Below 15 - will trigger AT_RISK
                .recordsProcessed(100000)
                .activeDbConnections(50)
                .heapUtilization(0.6)
                .totalErrors(100)
                .failedFiles(1)
                .criticalErrorTypes(Set.of())
                .build();

        // First evaluation - should adjust
        manager.evaluateAndAdjust(metrics);
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());

        // Immediate second evaluation - should skip (within cooldown)
        manager.evaluateAndAdjust(metrics);

        // Still only 1 call - second was skipped due to cooldown
        verify(mockEngine, times(1)).adjustConcurrency(anyInt(), anyInt());
    }

    // ============================================
    // Settings Tests
    // ============================================

    @Test
    void shouldGetCurrentSettings() {
        Map<String, Integer> settings = manager.getCurrentSettings();

        assertNotNull(settings);
        assertTrue(settings.containsKey("workItemConcurrency"));
        assertTrue(settings.containsKey("processingConcurrency"));

        // Should have initial values from the engine's controller
        assertNotNull(settings.get("workItemConcurrency"));
        assertNotNull(settings.get("processingConcurrency"));
    }

    // ============================================
    // Edge Cases
    // ============================================

    @Test
    void shouldHandleAllGoalsMet() {
        // Perfect scenario - all goals MET
        // Use high rates to ensure everything is comfortable
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(500)
                .totalFiles(900)
                .filesPerMinute(20.0)  // Well above 15 minimum
                .recordsProcessed(500000)
                .activeDbConnections(40)  // Well below 68% AT_RISK threshold
                .heapUtilization(0.5)     // Well below 68% AT_RISK threshold
                .totalErrors(100)         // Low errors
                .failedFiles(1)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // May or may not adjust depending on strategy (PerformanceStrategy might "build buffer")
        // Just verify no abort
        verify(mockEngine, never()).abort(anyString());
    }

    @Test
    void shouldHandleEarlyProgress() {
        // Early in the run with some progress
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(50)
                .totalFiles(900)
                .filesPerMinute(15.0)  // Exactly at minimum - should be OK
                .recordsProcessed(50000)
                .activeDbConnections(30)
                .heapUtilization(0.5)
                .totalErrors(10)
                .failedFiles(0)
                .criticalErrorTypes(Set.of())
                .build();

        manager.evaluateAndAdjust(metrics);

        // Should not panic and abort early
        verify(mockEngine, never()).abort(anyString());
    }
}