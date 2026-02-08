package cdc.domain;

/**
 * A legal entity (corporate, sponsor, issuer, fund, etc.).
 */
public record Account(
    String accountId,
    String name,
    String parentAccountId,
    String industryId,
    String region,
    String segment
) {}
