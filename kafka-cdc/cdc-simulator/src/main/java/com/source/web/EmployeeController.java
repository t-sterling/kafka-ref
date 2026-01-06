package com.source.web;

import com.source.domain.Employee;
import com.source.repo.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/")
public class EmployeeController {

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeController.class);

    private final EmployeeRepository repo;

    public EmployeeController(EmployeeRepository repo) {
        this.repo = repo;
    }

    @GetMapping("{entity}/{id}")
    public ResponseEntity<Employee> getById(@PathVariable("entity") String entity,
                                            @PathVariable("id") String id) {

        LOG.info("request for: /{}/{}", entity, id);

        return repo.get(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
