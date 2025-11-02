package com.sephora.services.ismsearchpoc.ism.model;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * DTO for Skuloc CSV records
 * Represents the 10-field CSV format from MCS
 */
public class SkulocRecord {
    
    private Long skuId;
    private Integer locationNumber;
    private Long availableQty;
    private Long merchReserveQty;
    private Long lostFoundQty;
    private String currencyCode;
    private BigDecimal wacInBase;
    private BigDecimal wacInAlternate;
    private LocalDate startDate;
    private LocalDate endDate;
    
    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public Long getSkuId() { return skuId; }
    public Integer getLocationNumber() { return locationNumber; }
    public Long getAvailableQty() { return availableQty; }
    public Long getMerchReserveQty() { return merchReserveQty; }
    public Long getLostFoundQty() { return lostFoundQty; }
    public String getCurrencyCode() { return currencyCode; }
    public BigDecimal getWacInBase() { return wacInBase; }
    public BigDecimal getWacInAlternate() { return wacInAlternate; }
    public LocalDate getStartDate() { return startDate; }
    public LocalDate getEndDate() { return endDate; }
    
    // Setters
    public void setSkuId(Long skuId) { this.skuId = skuId; }
    public void setLocationNumber(Integer locationNumber) { this.locationNumber = locationNumber; }
    public void setAvailableQty(Long availableQty) { this.availableQty = availableQty; }
    public void setMerchReserveQty(Long merchReserveQty) { this.merchReserveQty = merchReserveQty; }
    public void setLostFoundQty(Long lostFoundQty) { this.lostFoundQty = lostFoundQty; }
    public void setCurrencyCode(String currencyCode) { this.currencyCode = currencyCode; }
    public void setWacInBase(BigDecimal wacInBase) { this.wacInBase = wacInBase; }
    public void setWacInAlternate(BigDecimal wacInAlternate) { this.wacInAlternate = wacInAlternate; }
    public void setStartDate(LocalDate startDate) { this.startDate = startDate; }
    public void setEndDate(LocalDate endDate) { this.endDate = endDate; }
    
    // Builder class
    public static class Builder {
        private final SkulocRecord record = new SkulocRecord();
        
        public Builder skuId(Long skuId) {
            record.skuId = skuId;
            return this;
        }
        
        public Builder locationNumber(Integer locationNumber) {
            record.locationNumber = locationNumber;
            return this;
        }
        
        public Builder availableQty(Long availableQty) {
            record.availableQty = availableQty;
            return this;
        }
        
        public Builder merchReserveQty(Long merchReserveQty) {
            record.merchReserveQty = merchReserveQty;
            return this;
        }
        
        public Builder lostFoundQty(Long lostFoundQty) {
            record.lostFoundQty = lostFoundQty;
            return this;
        }
        
        public Builder currencyCode(String currencyCode) {
            record.currencyCode = currencyCode;
            return this;
        }
        
        public Builder wacInBase(BigDecimal wacInBase) {
            record.wacInBase = wacInBase;
            return this;
        }
        
        public Builder wacInAlternate(BigDecimal wacInAlternate) {
            record.wacInAlternate = wacInAlternate;
            return this;
        }
        
        public Builder startDate(LocalDate startDate) {
            record.startDate = startDate;
            return this;
        }
        
        public Builder endDate(LocalDate endDate) {
            record.endDate = endDate;
            return this;
        }
        
        public SkulocRecord build() {
            return record;
        }
    }
    
    @Override
    public String toString() {
        return "SkulocRecord{" +
                "skuId=" + skuId +
                ", locationNumber=" + locationNumber +
                ", availableQty=" + availableQty +
                ", merchReserveQty=" + merchReserveQty +
                ", lostFoundQty=" + lostFoundQty +
                ", currencyCode='" + currencyCode + '\'' +
                ", wacInBase=" + wacInBase +
                ", wacInAlternate=" + wacInAlternate +
                ", startDate=" + startDate +
                ", endDate=" + endDate +
                '}';
    }
}
