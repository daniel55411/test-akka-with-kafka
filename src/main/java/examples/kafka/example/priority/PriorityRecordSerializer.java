package examples.kafka.example.priority;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class PriorityRecordSerializer implements Serializer<PriorityRecord<String>> {
    private final static StringSerializer serializer = new StringSerializer();
    private final static MergeBuffer mergeBuffer = new MergeBuffer();

    private boolean isKey;

    @Override
    public void configure(Map<String, ?> map, boolean isKey) {
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, PriorityRecord<String> record) {
        if (record == null) {
            return null;
        }

        byte[] first = serializer.serialize(topic, record.getApiKey());
        byte[] second = serializer.serialize(topic, record.getValue());

        return mergeBuffer.merge(first, second);
    }

    @Override
    public void close() {

    }
}
