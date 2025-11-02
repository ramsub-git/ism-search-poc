package com.sephora.services.ismsearchpoc.framework.model;

/**
 * Result of processing a single item
 */
public class ProcessingResult<R> {
    
    private final boolean success;
    private final R result;
    private final Throwable error;
    
    private ProcessingResult(boolean success, R result, Throwable error) {
        this.success = success;
        this.result = result;
        this.error = error;
    }
    
    public static <R> ProcessingResult<R> success(R result) {
        return new ProcessingResult<>(true, result, null);
    }
    
    public static <R> ProcessingResult<R> failure(Throwable error) {
        return new ProcessingResult<>(false, null, error);
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public R getResult() {
        return result;
    }
    
    public Throwable getError() {
        return error;
    }
}
