package cdc.domain;

import java.time.LocalDate;

/**
 * An executed (or executing) transaction, often derived from an Opportunity.
 */
public record Deal(
    String dealId,
    String opportunityId,
    String accountId,
    String name,
    String status,
    String productId,
    LocalDate closeDate
) {}
