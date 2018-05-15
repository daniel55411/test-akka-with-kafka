package examples.kafka.example.priority;

public class PriorityRecord<T> {
    private final String apiKey;
    private final T value;

    public PriorityRecord(String apiKey, T value) {
        this.apiKey = apiKey;
        this.value = value;
    }

    public String getApiKey() {
        return apiKey;
    }

    public T getValue() {
        return value;
    }
}
