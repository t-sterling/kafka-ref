package cdc.domain;

/**
 * Join entity mapping an Account to internal coverage bankers/teams.
 */
public record CoverageTeam(
    String coverageTeamId,
    String accountId,
    String userId,
    String role,
    boolean primary
) {}
