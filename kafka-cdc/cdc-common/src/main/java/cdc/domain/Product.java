package cdc.domain;

/**
 * Banking product taxonomy (M&A, DCM, ECM, LevFin, etc.).
 */
public record Product(
    String productId,
    String code,
    String name
) {}
