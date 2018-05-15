package examples.kafka.example.priority;

import javafx.util.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class PriorityRecordDeserializer implements Deserializer<PriorityRecord<String>> {
    private final static StringDeserializer deserializer = new StringDeserializer();
    private final static MergeBuffer mergeBuffer = new MergeBuffer();

    private boolean isKey;

    @Override
    public void configure(Map<String, ?> map, boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public PriorityRecord<String> deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        Pair<byte[], byte[]> pair = mergeBuffer.separate(bytes);
        String apiKey = deserializer.deserialize(topic, pair.getKey());
        String value = deserializer.deserialize(topic, pair.getValue());

        return new PriorityRecord<>(apiKey, value);
    }

    @Override
    public void close() {

    }
}
