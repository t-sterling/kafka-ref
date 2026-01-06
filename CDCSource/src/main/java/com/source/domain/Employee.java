package com.source.domain;

import java.time.LocalDate;

public class Employee {

    public String recordId;     // e.g. EMP-000123
    public String firstName;
    public String lastName;
    public String addressLine1;
    public String city;
    public String state;
    public String zip;
    public String phoneNumber;
    public String managerId;    // recordId of another employee (or null)
    public LocalDate startDate;

    public long lastModifiedEpochMs;

    public Employee() {}
}
