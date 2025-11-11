package com.sephora.services.ismsearchpoc.processing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method that returns a business identifier component for this entity.
 * <p>
 * Multiple methods can be annotated, and the framework will combine them
 * into a single formatted business identifier string.
 * <p>
 * Example usage:
 * <pre>
 * @Entity
 * public class Skuloc {
 *
 *     @EmbeddedId
 *     private SkulocPK id;
 *
 *     @BusinessIDGetter("SKU")
 *     public Long getSkuId() {
 *         return id != null ? id.getSkuId() : null;
 *     }
 *
 *     @BusinessIDGetter("LOC")
 *     public Integer getLocationNumber() {
 *         return id != null ? id.getLocationNumber() : null;
 *     }
 * }
 * </pre>
 * <p>
 * The framework will automatically format this as: "SKU:123456|LOC:1001"
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface BusinessIDGetter {
    /**
     * The business identifier type (e.g., "SKU", "LOC", "ORDER").
     * This will be used as a prefix in the combined business identifier.
     */
    String value();
}