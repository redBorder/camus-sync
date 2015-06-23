package net.redborder.camus;

import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigFile {
    Map<String, Object> general;
    Map<String, Object> dimensionsPerTopic;
    List<String> defaultDimensions;

    public ConfigFile(String filePath) throws FileNotFoundException {
        general = (Map<String, Object>) Yaml.load(new File(filePath));
        dimensionsPerTopic = (Map<String, Object>) general.get("topics");
        defaultDimensions = (List<String>) dimensionsPerTopic.get("default");
    }

    public List<String> getDimensionsFromTopic(String topic) {
        List<String> dimensions = (List<String>) dimensionsPerTopic.get(topic);

        if (dimensions == null) {
            dimensions = defaultDimensions;
        }

        return dimensions;
    }

    public Set<String> getTopics() {
        Set<String> topics = dimensionsPerTopic.keySet();
        topics.remove("default");
        return topics;
    }

    public Object get(String key) {
        return general.get(key);
    }
}
