package clj_headlights;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A Coder using the JSON serialization format.
 *
 * <p>Each instance of {@code JsonCoder<T>} encodes and decodes instances
 * of type {@code T}.
 *
 * <p>To use, specify the {@code Coder} type on a PCollection:
 * <pre>
 * {@code
 * PCollection<MyCustomElement> records =
 *     input.apply(...)
 *          .setCoder(JsonCoder.of(MyCustomElement.class);
 * }
 * </pre>
 *
 * @param <T> the type of elements handled by this coder
 */
public class JsonCoder<T> extends CustomCoder<T> {
    private static final ObjectMapper mapper = new ObjectMapper();
    private StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private Class<T> type;

    public static <T> JsonCoder<T> of(Class<T> type) {
        return new JsonCoder(type);
    }

    public JsonCoder(Class<T> type) {
        this.type = type;
    }

    public void encode(T value, OutputStream out) throws IOException {
        String data = mapper.writeValueAsString(value);
        stringCoder.encode(data, out);
    }

    public T decode(InputStream in) throws IOException {
        String data = stringCoder.decode(in);
        return mapper.readValue(data, type);
    }

    public void verifyDeterministic() throws Coder.NonDeterministicException {
        throw new Coder.NonDeterministicException(this, "not determistic");
    }
}
