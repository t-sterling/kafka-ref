package cdc.domain;

/**
 * A relationship describing a Contact's role relative to an Account.
 */
public record AccountContactRole(
    String accountContactRoleId,
    String accountId,
    String contactId,
    String roleName,
    boolean primary
) {}
