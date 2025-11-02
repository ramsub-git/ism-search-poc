package com.sephora.services.ismsearchpoc.framework.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Context object for passing data through execution pipeline
 * Thread-safe for concurrent access
 */
public class ExecutionContext {
    
    private final Map<String, Object> data = new ConcurrentHashMap<>();
    
    public void put(String key, Object value) {
        data.put(key, value);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) data.get(key);
    }
    
    public <T> T get(String key, T defaultValue) {
        T value = get(key);
        return value != null ? value : defaultValue;
    }
    
    public boolean contains(String key) {
        return data.containsKey(key);
    }
    
    public void remove(String key) {
        data.remove(key);
    }
    
    public Map<String, Object> getAll() {
        return new ConcurrentHashMap<>(data);
    }
}
