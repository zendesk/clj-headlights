package clj_headlights;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import org.apache.beam.sdk.transforms.DoFn;

public abstract class AbstractCljDoFn<InputT, OutputT> extends DoFn<InputT, OutputT> {
  protected Object name;
  protected Object cljCall;
  protected Object inputExtractor;
  protected Object creationStack;
  protected IFn processElementFn;
  protected IFn setup;
  protected Object inputCoder;
  private Symbol pardo_ns = (Symbol)Clojure.var("clojure.core","symbol").invoke("clj-headlights.pardo");

  public AbstractCljDoFn(Object aname, Object ainputCoder, Object acljCall) {
    this.name = aname;
    this.inputCoder = ainputCoder;
    this.cljCall = acljCall;
    this.creationStack = Thread.currentThread().getStackTrace();
  }

  @Setup
  public void setup() {
    if (processElementFn == null) {
      processElementFn = Clojure.var("clj-headlights.pardo","process-element");
      setup = Clojure.var("clj-headlights.pardo", "setup");
    }
    System.ensureInitialized(pardo_ns);
    inputExtractor = setup.invoke(inputCoder, cljCall);
  }
}
