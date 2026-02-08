package cdc.domain;

import java.time.Instant;

/**
 * A potential deal / piece of pipeline (M&A, DCM, ECM, LevFin, etc.).
 */
public record Opportunity(
    String opportunityId,
    String accountId,
    String name,
    String stage,
    String productId,
    Double amount,
    String currency,
    Instant createdAt,
    Instant updatedAt
) {}
