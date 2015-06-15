package net.redborder.synchdfs;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public void deduplicate() {
        log.info("Deduplicating slot for topic {} at time {}", topic, time);
    }
}
