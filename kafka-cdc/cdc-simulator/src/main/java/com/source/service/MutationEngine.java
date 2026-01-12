package com.source.service;


import com.source.domain.CdcEvent;
import com.source.domain.Employee;
import com.source.domain.GapInfo;
import com.source.repo.EmployeeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
@EnableScheduling
public class MutationEngine {

    private static final Logger LOG = LoggerFactory.getLogger(MutationEngine.class);

    private final EmployeeRepository repo;
    private final CdcPublisher publisher;

    private final int minPerSec;
    private final int maxPerSec;
    private final boolean gapEnabled;
    private final double gapProbability;

    private final Random rnd = new Random();
    private final AtomicLong replayId = new AtomicLong(1000);

    public MutationEngine(EmployeeRepository repo,
                             CdcPublisher publisher,
                             @Value("${app.cdc.min-events-per-second:1}") int minPerSec,
                             @Value("${app.cdc.max-events-per-second:5}") int maxPerSec,
                             @Value("${app.cdc.gap.enabled:true}") boolean gapEnabled,
                             @Value("${app.cdc.gap.probability:0.01}") double gapProbability) {
        this.repo = repo;
        this.publisher = publisher;
        this.minPerSec = minPerSec;
        this.maxPerSec = maxPerSec;
        this.gapEnabled = gapEnabled;
        this.gapProbability = gapProbability;
    }

    @Scheduled(fixedRate = 1000)
    public void tick() {
        int rate = minPerSec + rnd.nextInt(Math.max(1, (maxPerSec - minPerSec + 1)));
        for (int i = 0; i < rate; i++) {
            produceOne();
        }
    }

    private void produceOne() {
        List<String> ids = repo.allIdsSnapshot();
        if (ids.isEmpty()) return;

        String id = ids.get(rnd.nextInt(ids.size()));
        Employee e = repo.get(id).orElse(null);
        if (e == null) return;

        boolean emitGap = gapEnabled && rnd.nextDouble() < gapProbability;

        if (emitGap) {

            long from = replayId.incrementAndGet();
            long to = from + (1 + rnd.nextInt(3)); // create a small gap
            replayId.set(to);

            CdcEvent gap = new CdcEvent();
            gap.eventId = UUID.randomUUID().toString();
            gap.entity = "Employee";
            gap.recordId = e.recordId;
            gap.type = "G";
            gap.timestamp = Instant.now().getEpochSecond();
            gap.replayId = to;
            gap.changedFields = Map.of();
            gap.gap = new GapInfo(from, to, "Simulated replayId gap");

            LOG.info("G: {}/{}", gap.recordId, gap.eventId);

            publisher.publish(e.recordId, gap);
            return;
        }

        Map<String, Object> changed = mutateEmployee(e);

        // If nothing changed (rare), skip
        if (changed.isEmpty()) return;

        long r = replayId.incrementAndGet();

        CdcEvent normal = new CdcEvent();
        normal.eventId = UUID.randomUUID().toString();
        normal.entity = "Employee";
        normal.recordId = e.recordId;
        normal.type = "N";
        normal.timestamp = Instant.now().getEpochSecond();
        normal.replayId = r;
        normal.changedFields = changed;

        LOG.info("N: {}/{}", normal.recordId, normal.eventId);
        publisher.publish(e.recordId, normal);
    }

    private Map<String, Object> mutateEmployee(Employee e) {
        Map<String, Object> changed = new LinkedHashMap<>();

        // choose 1-3 fields to change
        int fields = 1 + rnd.nextInt(3);
        for (int i = 0; i < fields; i++) {
            int pick = rnd.nextInt(5);
            switch (pick) {
                case 0 -> {
                    String newPhone = String.format("555-%03d-%04d", rnd.nextInt(1000), rnd.nextInt(10000));
                    if (!newPhone.equals(e.phoneNumber)) {
                        e.phoneNumber = newPhone;
                        changed.put("phoneNumber", newPhone);
                    }
                }
                case 1 -> {
                    String newAddr = (100 + rnd.nextInt(900)) + " " + (rnd.nextBoolean() ? "Main" : "Oak") + " St";
                    if (!newAddr.equals(e.addressLine1)) {
                        e.addressLine1 = newAddr;
                        changed.put("addressLine1", newAddr);
                    }
                }
                case 2 -> {
                    String newCity = rnd.nextBoolean() ? "Kennett Square" : "West Chester";
                    if (!newCity.equals(e.city)) {
                        e.city = newCity;
                        changed.put("city", newCity);
                    }
                }
                case 3 -> {
                    String newZip = String.format("%05d", 10000 + rnd.nextInt(89999));
                    if (!newZip.equals(e.zip)) {
                        e.zip = newZip;
                        changed.put("zip", newZip);
                    }
                }
                case 4 -> {
                    // manager changes occasionally
                    if (rnd.nextDouble() < 0.3) {
                        List<String> ids = repo.allIdsSnapshot();
                        String newMgr = ids.get(rnd.nextInt(ids.size()));
                        if (newMgr.equals(e.recordId)) newMgr = null;
                        if (!Objects.equals(newMgr, e.managerId)) {
                            e.managerId = newMgr;
                            changed.put("managerId", newMgr);
                        }
                    }
                }
            }
        }

        if (!changed.isEmpty()) {
            e.lastModifiedEpochMs = System.currentTimeMillis();
            changed.put("lastModifiedEpochMs", e.lastModifiedEpochMs);
        }

        return changed;
    }
}
