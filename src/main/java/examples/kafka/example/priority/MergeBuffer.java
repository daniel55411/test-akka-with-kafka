package examples.kafka.example.priority;

import javafx.util.Pair;

import java.nio.ByteBuffer;

public class MergeBuffer {
    public byte[] merge(byte[] first, byte[] second) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Integer.SIZE / Byte.SIZE + first.length + second.length);

        buffer.putInt(first.length);
        buffer.put(first);
        buffer.putInt(second.length);
        buffer.put(second);

        return buffer.array();
    }

    public Pair<byte[], byte[]> separate(byte[] data) {
        int length;
        ByteBuffer buffer = ByteBuffer.allocate(data.length);

        length = buffer.getInt();
        byte[] first = new byte[length];
        buffer.get(first, 0, length);

        length = buffer.getInt();
        byte[] second = new byte[length];
        buffer.get(second, 0, length);

        return new Pair<>(first, second);
    }
}
