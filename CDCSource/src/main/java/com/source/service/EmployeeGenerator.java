package com.source.service;

import com.source.domain.Employee;
import com.source.repo.EmployeeRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;

@Component
public class EmployeeGenerator {

    private final EmployeeRepository repo;
    private final int initialCount;
    private final Random rnd = new Random();

    private static final List<String> FIRST = List.of("Tim","Alex","Sam","Jordan","Taylor","Chris","Pat","Morgan","Casey","Jamie");
    private static final List<String> LAST  = List.of("Smith","Johnson","Brown","Davis","Miller","Wilson","Moore","Taylor","Anderson","Thomas");
    private static final List<String> STATES = List.of("PA","NY","NJ","DE","MD","VA");

    public EmployeeGenerator(EmployeeRepository repo,
                             @Value("${app.cdc.initial-count:2000}") int initialCount) {
        this.repo = repo;
        this.initialCount = initialCount;
    }

    @PostConstruct
    public void init() {
        // Create employees first without managers
        for (int i = 1; i <= initialCount; i++) {
            Employee e = randomEmployee(i);
            repo.put(e);
        }

        // Assign managers (some null)
        var ids = repo.allIdsSnapshot();
        for (String id : ids) {
            repo.get(id).ifPresent(e -> {
                if (rnd.nextDouble() < 0.2) { // 20% no manager
                    e.managerId = null;
                } else {
                    e.managerId = ids.get(rnd.nextInt(ids.size()));
                    if (e.managerId.equals(e.recordId)) e.managerId = null;
                }
            });
        }

        System.out.println("Generated employees: " + repo.size());
    }

    private Employee randomEmployee(int i) {
        Employee e = new Employee();
        e.recordId = String.format("EMP-%06d", i);
        e.firstName = FIRST.get(rnd.nextInt(FIRST.size()));
        e.lastName = LAST.get(rnd.nextInt(LAST.size()));
        e.addressLine1 = (100 + rnd.nextInt(900)) + " " + LAST.get(rnd.nextInt(LAST.size())) + " St";
        e.city = "Kennett Square";
        e.state = STATES.get(rnd.nextInt(STATES.size()));
        e.zip = String.format("%05d", 10000 + rnd.nextInt(89999));
        e.phoneNumber = String.format("555-%03d-%04d", rnd.nextInt(1000), rnd.nextInt(10000));
        e.startDate = LocalDate.now().minusDays(rnd.nextInt(3650)); // up to ~10y
        e.lastModifiedEpochMs = System.currentTimeMillis();
        return e;
    }
}
