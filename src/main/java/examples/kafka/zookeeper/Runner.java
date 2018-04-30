package examples.kafka.zookeeper;

import examples.kafka.zookeeper.flow.ConfigLoader;
import examples.kafka.zookeeper.flow.PropertiesConverter;
import examples.kafka.zookeeper.flow.PropertiesParser;
import examples.kafka.zookeeper.flow.SettingsReader;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jdk.
 * Date: 29.04.18
 */
public class Runner {
    private final static String PROPERTIES_STRING = "key = 123\nla.la = yes\nroot = 1\nroot.asd = 2\nla.la.ha = no";
    private final static String CONF_STRING = " system.dev { db { example = exam\n host=new}\n example { asd = ha } } ";

    public static void main(String[] args) throws Exception {
        PropertiesParser parser = new PropertiesParser();
        List<Map.Entry<String, byte[]>> entries =
                parser.parseAndSort(CONF_STRING);

        PropertiesConverter converter = new PropertiesConverter();
        entries = converter.convertToFilepath(entries);

        System.out.println("Parsed data");
        entries.forEach(entry -> System.out.println(entry.getKey() + " " + new String(entry.getValue())));
        System.out.println();

        ConfigLoader configLoader = new ConfigLoader("");
        configLoader.createConfigs(entries);
        System.out.println("Data is loaded");
        System.out.println();

        SettingsReader settingsReader = new SettingsReader("system/dev");
        System.out.println("Zookeeper's data");

        System.out.println(new String(settingsReader.getData("/db/example")));
        System.out.println(new String(settingsReader.getData("/db/host")));
        System.out.println(new String(settingsReader.getData("/example/asd")));
        System.out.println();

        System.out.println("After updating");

        ImmutableList<Map.Entry<String, byte[]>> withErrors = ImmutableList
                .of(new AbstractMap.SimpleEntry<>("/system/dev/db/host", "test".getBytes()));

        configLoader.updateConfigs(withErrors);
        System.out.println(new String(settingsReader.getData("/db/example")));
        System.out.println(new String(settingsReader.getData("/db/host")));
        System.out.println(new String(settingsReader.getData("/example/asd")));

    }
}
