package cdc.gap.handler.domain;

public record FillCommand(
    String recordId,
    String entityType
) {}
