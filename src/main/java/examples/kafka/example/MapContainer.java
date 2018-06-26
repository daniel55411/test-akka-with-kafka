package examples.kafka.example;

import java.util.HashMap;
import java.util.Map;

public class MapContainer implements Container {
    private final Map<Class, Object> instances;

    public MapContainer() {
        this.instances = new HashMap<>();
    }

    @Override
    public <T> T get(Class<T> c) {
        if (!instances.containsKey(c)) {
            throw new IllegalArgumentException("Class " + c.toString() + " not found");
        }

        return (T) instances.get(c);
    }

    @Override
    public <T> void set(T t) {
        set(t.getClass(), t);
    }

    @Override
    public <T> void set(Class<? extends T> aClass, T t) {
        System.out.println(aClass);
        instances.put(aClass, t);
    }
}
