package clj_headlights;

import clojure.java.api.Clojure;
import clojure.lang.Symbol;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.Semaphore;

public class System {
    private static Semaphore requireSem = new Semaphore(1);
    private static Semaphore waitForRetrySem = new Semaphore(0);
    private static HashSet loadedLibs = new HashSet();

    /**
     * Ensure that a namespace is properly required. Code loading is not thread safe so we need to protect
     * initialization with a semaphore that has a single slot, as we only want to load the code a single time. All
     * threads who did not get the semaphore spin-wait until the system is ready (as presumably the thread that *did*
     * get it is busy loading code). There's an obvious potential bug here, if the thread that got the semaphore dies
     * then the whole worker will die.
     */
    public static void ensureInitialized(Symbol ns) {
        if (!loadedLibs.contains(ns)) {
            if (requireSem.tryAcquire()) {
                try {
                    callback("pre-namespace-require", ns);
                    Clojure.var("clojure.core", "require").invoke(ns);
                    loadedLibs.addAll((Collection) Clojure.var("clojure.core", "loaded-libs").invoke());
                } finally {
                    requireSem.release();
                    waitForRetrySem.release(waitForRetrySem.getQueueLength() + 10); // give waiting threads chance to see if their ns has been loaded
                }
            } else {
                try {
                    waitForRetrySem.acquire();
                    ensureInitialized(ns);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void callback(String name, Object... arguments) {
        Clojure.var("clojure.core", "require").invoke(Symbol.intern("clj-headlights.system"));
        Clojure.var("clj-headlights.system", "callback").invoke(name, arguments);
    }
}
