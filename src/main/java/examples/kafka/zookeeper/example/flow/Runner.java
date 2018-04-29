package examples.kafka.zookeeper.example.flow;

import com.typesafe.config.ConfigValue;

import java.util.List;
import java.util.Map;

/**
 * Created by jdk.
 * Date: 29.04.18
 */
public class Runner {
    private final static String PROPERTIES_STRING = "key = 123\nla.la = yes\nroot = 1\nroot.asd = 2\nla.la.ha = no";
    private final static String CONF_STRING = " system.dev { db { example = 3\n host=local}\n example { asd = ha } } ";

    public static void main(String[] args) throws Exception {
        PropertiesParser parser = new PropertiesParser();
        List<Map.Entry<String, ConfigValue>> entries =
                parser.parseAndSort(CONF_STRING);

        PropertiesConverter converter = new PropertiesConverter();
        entries = converter.convertToFilepath(entries);

        System.out.println("Parsed data");
        entries.forEach(entry -> System.out.println(entry.getKey() + " " + entry.getValue().unwrapped()));
        System.out.println();

        ConfigLoader configLoader = new ConfigLoader("");
        configLoader.loadConfig(entries);
        System.out.println("Data is loaded");
        System.out.println();

        SettingsReader settingsReader = new SettingsReader("system/dev/example");
        System.out.println("Zookeeper's data");

        System.out.println(new String(settingsReader.getData("/asd")));
//        System.out.println(new String(settingsReader.getData("/host")));

    }
}
