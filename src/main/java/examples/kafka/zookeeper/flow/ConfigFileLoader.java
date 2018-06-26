package examples.kafka.zookeeper.flow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class ConfigFileLoader {
    private final PropertiesParser parser;
    private final PropertiesConverter converter;
    private final ConfigLoader configLoader;

    public ConfigFileLoader() {
        this.parser = new PropertiesParser();
        this.converter = new PropertiesConverter();
        this.configLoader = new ConfigLoader("");
    }

    public void load(String path) throws IOException {
        File file = new File(path);
        String conf = String.join("\n", Files.readAllLines(file.toPath()));

        List<Map.Entry<String, byte[]>> entries =
                parser.parseAndSort(conf);

        load(entries);

    }

    private void load(List<Map.Entry<String, byte[]>> entries) {
        entries = converter.convertToFilepath(entries);
        configLoader.createConfigs(entries);
    }
}
