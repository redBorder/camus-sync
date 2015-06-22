package net.redborder.camus;

import org.ho.yaml.Yaml;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

public class DimensionsFile {
    Map<String, Object> dimensionsPerTopic;
    List<String> defaultDimensions;

    public DimensionsFile(String filePath) throws FileNotFoundException {
        dimensionsPerTopic = (Map<String, Object>) Yaml.load(new File(filePath));
        defaultDimensions = (List<String>) dimensionsPerTopic.get("default");
    }

    public List<String> getDimensionsFromTopic(String topic) {
        List<String> dimensions = (List<String>) dimensionsPerTopic.get(topic);

        if (dimensions == null) {
            dimensions = defaultDimensions;
        }

        return dimensions;
    }
}
