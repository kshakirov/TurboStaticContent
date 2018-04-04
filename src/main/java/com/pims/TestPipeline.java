package com.pims;

        import org.apache.beam.runners.dataflow.DataflowRunner;
        import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
        import org.apache.beam.sdk.Pipeline;
        import org.apache.beam.sdk.io.TextIO;
        import org.apache.beam.sdk.options.PipelineOptions;
        import org.apache.beam.sdk.options.PipelineOptionsFactory;
        import org.apache.beam.sdk.transforms.*;
        import org.apache.beam.sdk.values.KV;
        import org.apache.beam.sdk.io.redis.*;
        import org.apache.commons.io.FileUtils;


        import java.io.File;
        import java.io.FileNotFoundException;
        import java.io.FileOutputStream;
        import java.io.IOException;
        import java.util.Iterator;
        import java.util.regex.Pattern;


/**
 * Created by kshakirov on 2/23/18.
 */
public class TestPipeline {




    public static void main(String[] args) {

        Pipeline p = null;
        if(false) {
            DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
            options.setProject("skyuk-uk-nowtv-bit-ecc-dev");
            //options.setServiceAccount("nowtv-data-dev@skyuk-uk-nowtv-bit-ecc-dev.iam.gserviceaccount.com");
            options.setStagingLocation("gs://zoral-sky-dev/bg-temp-1");
            options.setGcpTempLocation("gs://zoral-sky-dev/gcp-temp-1");
            options.setRegion("europe-west1");
            options.setRunner(DataflowRunner.class);
        p = Pipeline.create(options);

        }else{
            PipelineOptions options = PipelineOptionsFactory.create();
            p = Pipeline.create(options);
        }


        p.apply(TextIO.read().from("sitemap.txt"))
                .apply("ExtractUrls", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element());
                    }
                }))
                .apply(Regex.find(Pattern.compile("/\\d+")))
                .apply(Regex.replaceFirst("/",""))
                .apply(new HttpFuncs.CreateRequestUrsl())
                .apply(ParDo.of(HttpFuncs.getHttpRequestRunnerFunc()))
                .apply(GroupByKey.create())
                .apply(ParDo.of(HtmlProcessor.createPartProcessor()));
        p.run().waitUntilFinish();
    }
}



