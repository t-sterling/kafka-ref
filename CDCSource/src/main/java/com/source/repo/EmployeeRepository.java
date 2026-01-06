package com.source.repo;


import com.source.domain.Employee;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class EmployeeRepository {

    private final Map<String, Employee> store = new ConcurrentHashMap<>();
    private final List<String> ids = Collections.synchronizedList(new ArrayList<>());

    public void put(Employee e) {
        store.put(e.recordId, e);
        // only add id once
        if (!ids.contains(e.recordId)) {
            ids.add(e.recordId);
        }
    }

    public Optional<Employee> get(String recordId) {
        return Optional.ofNullable(store.get(recordId));
    }

    public int size() {
        return store.size();
    }

    public List<String> allIdsSnapshot() {
        synchronized (ids) {
            return List.copyOf(ids);
        }
    }
}
