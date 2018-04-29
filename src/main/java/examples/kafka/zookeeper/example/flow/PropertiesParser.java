package examples.kafka.zookeeper.example.flow;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jdk.
 * Date: 29.04.18
 */
public class PropertiesParser {
    public List<Map.Entry<String, ConfigValue>> parseAndSort(String propertiesString) {
        Config config = ConfigFactory.parseString(propertiesString);

        return config
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .collect(Collectors.toList());
    }
}
