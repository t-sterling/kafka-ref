package cdc.gap.handler.domain;

public record Either<L, R> (L left, R right) {}
