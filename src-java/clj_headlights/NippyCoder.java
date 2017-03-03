package clj_headlights;

import clojure.java.api.Clojure;
import clojure.lang.Symbol;
import org.apache.beam.sdk.coders.AtomicCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default coder for all Clojure data structures. Not used for coding KV keys since it's not deterministic.
 */
public class NippyCoder extends AtomicCoder<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(NippyCoder.class);
    public static NippyCoder of() {
        return new NippyCoder();
    }
    Symbol nippy_ns = (Symbol)Clojure.var("clojure.core","symbol").invoke("clj-headlights.nippy");


    private void clojurePrint(OutputStream out, Object o) {
        System.ensureInitialized(nippy_ns);
        Clojure.var("clj-headlights.nippy","fast-encode-stream").invoke(out, o);
    }

    private Object clojureRead(InputStream in) {
        System.ensureInitialized(nippy_ns);
        return Clojure.var("clj-headlights.nippy","fast-decode-stream").invoke(in);
    }

    @Override
    public void encode(Object o, OutputStream out) throws IOException {
        clojurePrint(out, o);
    }

    @Override
    public Object decode(InputStream in) throws IOException {
        return clojureRead(in);
    }

    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this, "nippy is not deterministic");
    }
}
