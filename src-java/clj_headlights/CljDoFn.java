package clj_headlights;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;


public class CljDoFn<InputT, OutputT> extends AbstractCljDoFn<InputT, OutputT> {
  public CljDoFn(Object aname, Object ainputCoder, Object acljCall) {
      super(aname, ainputCoder, acljCall);
  }

  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) {
    processElementFn.invoke(context, window, cljCall, creationStack, inputExtractor, null);
  }
}
