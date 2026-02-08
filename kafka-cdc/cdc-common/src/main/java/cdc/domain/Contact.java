package cdc.domain;

/**
 * An individual person at an Account.
 */
public record Contact(
    String contactId,
    String accountId,
    String firstName,
    String lastName,
    String title,
    String email
) {
  public String fullName() {
    String fn = firstName == null ? "" : firstName.trim();
    String ln = lastName == null ? "" : lastName.trim();
    return (fn + " " + ln).trim();
  }
}
