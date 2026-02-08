package cdc.domain;

/**
 * Join entity between a Meeting (or Interaction) and an attendee.
 * Attendee can be internal (banker user) or external (client contact).
 */
public record MeetingAttendee(
    String meetingAttendeeId,
    String interactionId,
    String attendeeType,
    String attendeeId,
    boolean required
) {}
