package mastodonc;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

public class BaldrReaderTest {
    @Test
    public void testShouldDoSomething() throws IOException {
        String message = "Hello, world";
        byte[] recordBytes = createRecordBytes(message);

        ByteArrayInputStream stream = new ByteArrayInputStream(recordBytes);
        BaldrReader r = new BaldrReader(stream);

        BaldrRecord record = r.next();

        assertEquals("Hello, world", new String(record.bytes()));
        assertEquals("Hello, world".length(), record.length());
        assertEquals(0, record.position());
    }

    private byte[] createRecordBytes(String message) {
        ByteBuffer lengthByteBuf = ByteBuffer.allocate(8);
        lengthByteBuf.order(ByteOrder.BIG_ENDIAN);
        lengthByteBuf.putLong(message.length());
        byte[] lengthBytes = lengthByteBuf.array();

        byte[] messageBytes = message.getBytes();

        byte[] recordBytes = new byte[lengthBytes.length + messageBytes.length];
        ByteBuffer recordBuf = ByteBuffer.wrap(recordBytes);
        recordBuf.put(lengthBytes);
        recordBuf.put(messageBytes);
        return recordBytes;
    }
}
