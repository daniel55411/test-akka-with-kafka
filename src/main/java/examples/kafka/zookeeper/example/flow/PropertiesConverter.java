package examples.kafka.zookeeper.example.flow;


import com.typesafe.config.ConfigValue;
import org.apache.kafka.common.protocol.types.Field;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jdk.
 * Date: 29.04.18
 */
public class PropertiesConverter{
    public List<Map.Entry<String, ConfigValue>> convertToFilepath(
            List<Map.Entry<String, ConfigValue>> configs) {
        return configs
                .stream()
                .map(entry -> {
                    String path = "/" + entry.getKey().replaceAll("\\.", "/");
                    return new AbstractMap.SimpleEntry<>(path, entry.getValue());
                })
                .collect(Collectors.toList());
    }
}
