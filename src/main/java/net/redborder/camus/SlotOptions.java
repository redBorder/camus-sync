package net.redborder.camus;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SlotOptions {
    private static final Logger log = LoggerFactory.getLogger(SlotOptions.class);

    private final String camusPath;
    private final List<HdfsServer> servers;
    private final List<Slot> slots;
    private final Slot bestSlot;
    private final String topic;
    private final DateTime time;

    public SlotOptions(String camusPathStr, List<HdfsServer> servers, String topic, DateTime time) {
        this.camusPath = camusPathStr;
        this.servers = servers;
        this.topic = topic;
        this.time = time;
        this.slots = loadSlots();
        this.bestSlot = Collections.max(slots);
    }

    public String getTopic() {
        return topic;
    }

    public DateTime getTime() {
        return time;
    }

    private List<Slot> loadSlots() {
        List<Slot> createdSlots = new ArrayList<>();

        for (HdfsServer server: servers) {
            createdSlots.add(new Slot(camusPath, server, topic, time));
        }

        return createdSlots;
    }

    public void deduplicate(boolean dryrun, List<String> dimensions) {
        log.info("Deduplicate slot for topic {} at time {}", topic, time);
        List<String> paths = new ArrayList<>();
        Set<Long> randomNumberSet = new HashSet<>();
        Set<Long> eventsSet = new HashSet<>();

        for (Slot slot : slots) {
            if (slot.getPaths().isEmpty()) {
                log.info("|-- [!!] Empty or missing slot at path {}", slot.getFullFolder());
            } else {
                log.info("|-- Found slot at path {}", slot.getFullFolder());
                paths.add(slot.getFullFolder());
            }

            randomNumberSet.add(slot.getRandomNumber());
            eventsSet.add(slot.getEvents());
        }

        if (paths.isEmpty()) {
            log.info("No paths available to deduplicate, ignoring...");
        } else if ((randomNumberSet.size() == 1 && eventsSet.size() == 1)) {
            log.info("Slot for topic {} time {} is already deduplicated, ignoring...", topic, time);
        } else if (!dryrun) {
            DeduplicationJob pigJob = new DeduplicationJob(paths, dimensions);
            DeduplicationJob.Results results = pigJob.run();
            log.info("Written {} records into {}", results.getNumberRecords(), results.getPath().toString());

            if (results.getNumberRecords() == 0) {
                log.error("Pig deduplicate job did not write a damn single thing!");
                log.error("Aborting deduplicate job for topic {} time {}", topic, time);
                return;
            }

            UUID uuid = UUID.randomUUID();
            long randomNumber = Math.abs(uuid.getMostSignificantBits());

            for (Slot slot : slots) {
                slot.destroy();
                String uploadName = topic + ".0.0." + String.valueOf(results.getNumberRecords()) + "." + randomNumber + ".deduplicated.gz";
                slot.upload(results.getPath(), uploadName);
            }
        }
    }

    public void synchronize(boolean dryrun) {
        log.info("Synchronizing slot for topic {} at time {}", topic, time);

        Set<Long> events = new HashSet<>();
        for (Slot slot : slots) {
            events.add(slot.getEvents());
        }

        if (events.size() < 2) {
            log.info("Slot already in sync topic {} time {}", topic, time);
            return;
        }

        for (Slot slot : slots) {
            if (bestSlot == slot || bestSlot.getEvents() == slot.getEvents()) continue;
            log.info("Found differences between options | delta {} best {} current {}",
                    (bestSlot.getEvents() - slot.getEvents()),
                    bestSlot.getEvents(),
                    slot.getEvents());

            if (dryrun) continue;

            log.info("|-- Synchronizing from {} to {}", bestSlot.getServer().getHostname(), slot.getServer().getHostname());
            slot.destroy();

            for (Path path : bestSlot.getPaths()) {
                slot.upload(path);
            }

            log.info("|-- Sync done at {}", slot.getServer().getHostname());
        }
    }
}
