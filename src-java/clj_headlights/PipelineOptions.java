package clj_headlights;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import java.util.Map;

/**
 * This interface is registered in PipelineOptionsFactory to provide an extensible generic storage for options.
 */
public interface PipelineOptions extends DataflowPipelineOptions {
    Map<String, Object> getCustomOptions();
    void setCustomOptions(Map<String, Object> value);
    String getVersion();
    void setVersion(String value);
}
