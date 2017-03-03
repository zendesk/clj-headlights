package clj_headlights;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Symbol;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class CljSerializableFunction implements SerializableFunction<Object, Object> {
  Object cljCall;
  IFn serializableFunctionApply;
  Symbol cljFnCallNs = (Symbol)Clojure.var("clojure.core","symbol").invoke("clj-headlights.clj-fn-call");


  public CljSerializableFunction(Object cljCall) {
    this.cljCall = cljCall;
  }

  @Override
  public Object apply(Object input) {
    if (serializableFunctionApply == null) {
      serializableFunctionApply = Clojure.var("clj-headlights.clj-fn-call", "serializable-function-apply");
    }
    System.ensureInitialized(cljFnCallNs);

    return serializableFunctionApply.invoke(cljCall,input);
  }
}
