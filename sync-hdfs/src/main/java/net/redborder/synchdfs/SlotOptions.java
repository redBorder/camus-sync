package net.redborder.synchdfs;

import net.redborder.pig.DeduplicationJob;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SlotOptions {
    private static final Logger log = LoggerFactory.getLogger(SlotOptions.class);

    private final String camusPathStr;
    private final List<HdfsServer> servers;
    private final List<Slot> slots;
    private final List<Slot> unmodificableSlots;
    public final String topic;
    public final DateTime time;

    public SlotOptions(String camusPathStr, List<HdfsServer> servers, String topic, DateTime time) {
        this.camusPathStr = camusPathStr;
        this.servers = servers;
        this.topic = topic;
        this.time = time;
        this.slots = loadSlots();
        this.unmodificableSlots = Collections.unmodifiableList(slots);
    }

    private List<Slot> loadSlots() {
        List<Slot> createdSlots = new ArrayList<>();

        for (HdfsServer server: servers) {
            createdSlots.add(new Slot(camusPathStr, server, topic, time));
        }

        return createdSlots;
    }

    public List<Slot> getSlots() {
        return unmodificableSlots;
    }

    public Slot bestSlot() {
        return Collections.max(slots);
    }

    public void deduplicate(boolean dryrun, String[] dimensions) {
        log.info("Deduplicate slot for topic {} at time {}", topic, time);
        List<String> paths = new ArrayList<>();

        for (Slot slot : slots) {
            log.info("|-- Slot path {}", slot.getFolder());
            paths.add(slot.getFolder());
        }

        if (!dryrun) {
            DeduplicationJob pigJob = new DeduplicationJob(paths, dimensions);
            DeduplicationJob.Results results = pigJob.run();
            log.info("Written {} records into {}", results.getNumberRecords(), results.getPath().toString());

            for (Slot slot : slots) {
                slot.destroy();
                String uploadName = topic + ".0.0." + String.valueOf(results.getNumberRecords()) + ".without.duplicates.gz";
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
            log.info("slot already in sync topic {} time {}", topic, time);
            return;
        }

        Slot bestSlot = bestSlot();
        for (Slot slot : slots) {
            if (bestSlot == slot || bestSlot.getEvents() == slot.getEvents()) continue;
            log.info("found differences between options | delta {} best {} current {}",
                    (bestSlot.getEvents() - slot.getEvents()),
                    bestSlot.getEvents(),
                    slot.getEvents());

            if (dryrun) continue;

            log.info("|-- synchronizing from {} to {}", bestSlot.getServer().getHostname(), slot.getServer().getHostname());
            slot.destroy();

            for (Path path : bestSlot.getPaths()) {
                log.info("|-- uploading {} to {}", path.toString(), slot.getFolder());

                slot.upload(path);
            }

            log.info("|-- sync done at {}", slot.getServer().getHostname());
        }
    }
}
