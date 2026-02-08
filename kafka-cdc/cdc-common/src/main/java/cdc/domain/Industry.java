package cdc.domain;

/**
 * Industry / sector taxonomy (e.g., GICS or internal taxonomy).
 */
public record Industry(
    String industryId,
    String code,
    String name,
    String parentIndustryId
) {}
