package examples.kafka.example;

public interface Container {
    <T> T get(Class<T> c);
    <T> void set(T t);
    <T> void set(Class<? extends T> aClass, T t);
}
