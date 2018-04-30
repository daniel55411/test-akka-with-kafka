package examples.kafka.zookeeper.flow;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jdk.
 * Date: 29.04.18
 */
public class PropertiesParser {
    public List<Map.Entry<String, byte[]>> parseAndSort(String propertiesString) {
        Config config = ConfigFactory.parseString(propertiesString);

        return config
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> new AbstractMap.SimpleEntry<>(
                        entry.getKey(),
                        getBytes(entry.getValue()))
                )
                .collect(Collectors.toList());
    }


    private byte[] getBytes(ConfigValue configValue) {
        return configValue.unwrapped().toString().getBytes();
    }
}
