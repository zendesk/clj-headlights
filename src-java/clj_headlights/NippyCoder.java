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
    private final Symbol nippyNs = (Symbol)Clojure.var("clojure.core","symbol").invoke("clj-headlights.nippy");
    private final int bytesWrittenWarnThreshold;
    private final String name;

    private NippyCoder(String name, int bytesWrittenWarnThreshold) {
        this.name = name;
        this.bytesWrittenWarnThreshold = bytesWrittenWarnThreshold;
    }

    private NippyCoder(String name) {
        this(name, 104857600); // 100MB, recommended max by Google.
    }

    public NippyCoder() {
        this("unknown");
    }

    public static NippyCoder of(String name, int bytesWrittenWarnThreshold) {
        return new NippyCoder(name, bytesWrittenWarnThreshold);
    }

    public static NippyCoder of(String name) {
        return new NippyCoder(name);
    }

    public static NippyCoder of() {
        return new NippyCoder();
    }

    private void clojurePrint(OutputStream out, Object o) {
        System.ensureInitialized(nippyNs);
        Long bytesWritten = (Long) Clojure.var("clj-headlights.nippy","fast-encode-stream").invoke(out, o);
        if (bytesWritten > bytesWrittenWarnThreshold) {
            LOG.warn("NippyCoder \"{}\" wrote more than {} bytes: {} bytes written.", this.name, bytesWrittenWarnThreshold, bytesWritten);
        }
    }

    private Object clojureRead(InputStream in) {
        System.ensureInitialized(nippyNs);
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

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(this, "nippy is not deterministic");
    }
}
