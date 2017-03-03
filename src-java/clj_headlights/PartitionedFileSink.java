package clj_headlights;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.*;


public class PartitionedFileSink extends PTransform<PCollection<KV<String, Iterable<String>>>, PCollection<String>> {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSink.class);
    public static final String TEMPORARY_FILENAME_SEPARATOR = "/temp/";

    protected final String outputPath;
    protected final String filename;

    /**
     * Construct a PartitionedFileSink with the given base output path and filename
     */
    public PartitionedFileSink(String outputPath, String filename) {
        if(!outputPath.matches("^/.+|^gs://.+")){
            LOG.error("outputPath must be an absolute path");
        }
        LOG.info("Writing to: " + outputPath + "/{{ key }}/" + filename);
        this.outputPath = outputPath + (outputPath.endsWith("/") ? "" : "/");
        this.filename = filename;
    }

    @Override
    public PCollection<String> expand(PCollection<KV<String, Iterable<String>>> pInput) {
        return pInput.apply(ParDo.of(new BundleTempFileWriter(outputPath)))
                .apply(ParDo.of(new SuccessfulBundleCopier(outputPath, filename)));
    }

    public static class BundleTempFileWriter extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {
        protected final String baseTemporaryFilename;
        protected String bundleId;

        public BundleTempFileWriter(String baseOutputFilename) {
            this.baseTemporaryFilename = baseOutputFilename;
        }

        protected static final String buildTemporaryFilename(String prefix, String bundleID, String partition) {
            return prefix + partition + TEMPORARY_FILENAME_SEPARATOR + bundleID;
        }

        @StartBundle
        public void startBundle(StartBundleContext c) throws IOException {
            bundleId = UUID.randomUUID().toString();

        }

        private static ByteBuffer wrap(String value) throws Exception {
            return ByteBuffer.wrap((value + "\n").getBytes("UTF-8"));
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow w) throws Exception {
            KV<String, Iterable<String>> element = c.element();
            String partition = element.getKey();
            String tempfileName = buildTemporaryFilename(baseTemporaryFilename, bundleId, partition);
            ResourceId tempFileResource = FileSystems.matchNewResource(tempfileName, false);
            WritableByteChannel tempFileChannel = FileSystems.create(tempFileResource, MimeTypes.TEXT);
            for(String value : element.getValue()) {
                tempFileChannel.write(wrap(value));
            }
            tempFileChannel.close();
            c.output(KV.of(partition, tempFileResource.toString()));
        }
    }

    public static class SuccessfulBundleCopier extends DoFn<KV<String, String>, String> {
        private String outputPath;
        private String filename;

        public SuccessfulBundleCopier(String baseOutputFilename, String extension) {
            this.outputPath = baseOutputFilename;
            this.filename = extension;
        }
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String partition = c.element().getKey();
            try {
                ResourceId completedBundle = FileSystems.matchSingleFileSpec(c.element().getValue()).resourceId();
                ResourceId dest = FileSystems.matchNewResource(outputPath + partition + "/" + filename, false);

                LOG.info("Moving " + completedBundle.toString() + " to " + dest.toString());
                FileSystems.rename(Arrays.asList(completedBundle), Arrays.asList(dest), MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
                FileSystems.delete(
                        Arrays.asList(FileSystems.matchSingleFileSpec(outputPath + partition + TEMPORARY_FILENAME_SEPARATOR).resourceId()),
                        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
            }
            catch(FileNotFoundException e) {
                // just means this key has already been processed
            }
        }
    }
}
