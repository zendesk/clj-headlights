package clj_headlights;

import clojure.java.api.Clojure;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default coder for KV keys. Just uses read-string and pr-str to serialize
 * clojure data structures as edn. Uses the dataflow string coder to deal with the
 * bytes and IO nonsense.
 */
public class EdnCoder extends AtomicCoder<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(EdnCoder.class);
    public static EdnCoder of() {
        return new EdnCoder();
    }

    private StringUtf8Coder stringCoder = StringUtf8Coder.of();

    private String clojurePrint(Object o) {
        return (String) Clojure.var("clojure.core", "pr-str").invoke(o);
    }

    private Object clojureRead(String s) {
        return Clojure.var("clojure.core", "read-string").invoke(s);
    }

    public void encode(Object o, OutputStream out) throws IOException {
        String outString = clojurePrint(o);
        assert ((Boolean) Clojure.var("clojure.core","=").invoke(o, clojureRead(outString))) : outString + "\ndid not deserialize back to an equal object";
        stringCoder.encode(clojurePrint(o), out);
    }

    public Object decode(InputStream in) throws IOException {
        Object ret = null;
        String edn = stringCoder.decode(in);
        try {
            ret = clojureRead(edn);
        } catch (Exception e) {
            LOG.error("Exception encountered trying to decode edn:" + edn, e);
            throw e;
        }

        return ret;
    }

    public void verifyDeterministic() { }
}
