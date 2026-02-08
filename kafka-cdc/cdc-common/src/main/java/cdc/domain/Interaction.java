package cdc.domain;

import java.time.Instant;

/**
 * A logged client interaction (meeting, call, email, conference, etc.).
 */
public record Interaction(
    String interactionId,
    String accountId,
    String opportunityId,
    String type,
    String subject,
    Instant occurredAt,
    String createdByUserId
) {}
