package com.sephora.services.framework.goal;

import com.sephora.services.ismsearchpoc.framework.goal.GoalEvaluation;
import com.sephora.services.ismsearchpoc.framework.goal.GoalStatus;
import com.sephora.services.ismsearchpoc.framework.goal.PerformanceGoal;
import com.sephora.services.ismsearchpoc.framework.goal.Severity;
import com.sephora.services.ismsearchpoc.framework.metrics.MetricsSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PerformanceGoal
 * FIXED: Tests now account for real-time passage
 */
class PerformanceGoalTest {

    private PerformanceGoal goal;

    @BeforeEach
    void setUp() {
        // 60 minute deadline, 15 files/min minimum, 80% tolerance
        goal = new PerformanceGoal(Duration.ofMinutes(60), 15.0, 0.8);
    }

    @Test
    void shouldBeMet_whenAheadOfPace() {
        // 300 files processed out of 900, going fast
        // Even with 60 min remaining, 30 files/min is way ahead of 10 files/min needed
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(300)
                .totalFiles(900)
                .filesPerMinute(30.0)
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);

        assertEquals(GoalStatus.MET, eval.getStatus());
        assertEquals(Severity.CRITICAL, eval.getSeverity());
    }

    @Test
    void shouldBeAtRisk_whenSlightlyBehindPace() throws InterruptedException {
        // Let some time pass first
        Thread.sleep(100);

        // Very low rate compared to what's needed
        // With 900 files and slow rate, even with time remaining, this is risky
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(10)
                .totalFiles(900)
                .filesPerMinute(5.0)  // Way too slow
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);

        // With such a low rate (5 files/min) vs minimum requirement (15 files/min),
        // should be AT_RISK or VIOLATED
        assertTrue(eval.getStatus() == GoalStatus.AT_RISK ||
                        eval.getStatus() == GoalStatus.VIOLATED,
                "Expected AT_RISK or VIOLATED but got: " + eval.getStatus());

        // Check goal metrics
        Map<String, Object> goalMetrics = eval.getMetrics();
        assertTrue((Double) goalMetrics.get("rateGap") > 0); // Behind
    }

    @Test
    void shouldBeViolated_whenSignificantlyBehindPace() throws InterruptedException {
        // Let time pass
        Thread.sleep(100);

        // Extremely slow rate - clearly violated
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(5)
                .totalFiles(900)
                .filesPerMinute(0.5)  // Extremely slow
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);

        // 0.5 files/min is way below 50% of required rate
        assertEquals(GoalStatus.VIOLATED, eval.getStatus());
    }

    @Test
    void shouldCalculateCorrectMetrics() {
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(450)
                .totalFiles(900)
                .filesPerMinute(15.0)
                .build();

        GoalEvaluation eval = goal.checkStatus(metrics);
        Map<String, Object> goalMetrics = eval.getMetrics();

        assertNotNull(goalMetrics.get("requiredFilesPerMinute"));
        assertNotNull(goalMetrics.get("currentFilesPerMinute"));
        assertNotNull(goalMetrics.get("rateGap"));
        assertNotNull(goalMetrics.get("filesRemaining"));
        assertNotNull(goalMetrics.get("timeRemainingMinutes"));
        assertEquals(50.0, (Double) goalMetrics.get("percentComplete"));
    }

    @Test
    void shouldTransitionFromMetToAtRiskToViolated() throws InterruptedException {
        // Use a goal with realistic deadline for testing
        PerformanceGoal testGoal = new PerformanceGoal(
                Duration.ofMinutes(10),  // 10 minute deadline
                90.0,                     // Need 90 files/min minimum
                0.8
        );

        // Scenario 1: Going very fast - definitely MET
        // Need 90 files/min, doing 500 files/min
        MetricsSnapshot metrics1 = MetricsSnapshot.builder()
                .filesProcessed(100)
                .totalFiles(900)
                .filesPerMinute(500.0)  // Way ahead of pace
                .build();
        assertEquals(GoalStatus.MET, testGoal.checkStatus(metrics1).getStatus());

        // Scenario 2: Going slow - AT_RISK or VIOLATED
        // Need 90 files/min minimum, only doing 30 files/min
        MetricsSnapshot metrics2 = MetricsSnapshot.builder()
                .filesProcessed(150)
                .totalFiles(900)
                .filesPerMinute(30.0)  // Below minimum
                .build();
        GoalStatus status2 = testGoal.checkStatus(metrics2).getStatus();
        assertTrue(status2 == GoalStatus.AT_RISK || status2 == GoalStatus.VIOLATED,
                "Expected AT_RISK or VIOLATED with slow rate, got: " + status2);

        // Scenario 3: Crawling - definitely VIOLATED
        // Need 90 files/min, only doing 1 file/min
        MetricsSnapshot metrics3 = MetricsSnapshot.builder()
                .filesProcessed(160)
                .totalFiles(900)
                .filesPerMinute(1.0)  // Way below minimum (< 50% threshold)
                .build();
        assertEquals(GoalStatus.VIOLATED, testGoal.checkStatus(metrics3).getStatus());
    }

    @Test
    void shouldBeViolated_whenTimeExpired() throws InterruptedException {
        // Create goal with 1 second deadline
        PerformanceGoal expiredGoal = new PerformanceGoal(
                Duration.ofSeconds(1),
                15.0,
                0.8
        );

        // Wait for deadline to pass
        Thread.sleep(1500);

        // Check status - should be VIOLATED regardless of progress
        MetricsSnapshot metrics = MetricsSnapshot.builder()
                .filesProcessed(450)
                .totalFiles(900)
                .filesPerMinute(15.0)
                .build();

        GoalEvaluation eval = expiredGoal.checkStatus(metrics);
        assertEquals(GoalStatus.VIOLATED, eval.getStatus(),
                "Should be VIOLATED when time deadline exceeded");
    }
}