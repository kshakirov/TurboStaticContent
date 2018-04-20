package com.pims;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.regex.Pattern;


/**
 * Created by kshakirov on 2/23/18.
 */
public class StaticPagesPipeline {


    public static void main(String[] args) {

        PipelineConfig pipelineConfig = ConfigReader.getConfig();
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> keyCollection = pipeline.apply(TextIO.read().from(pipelineConfig.parseFile))
                .apply("ExtractUrls", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(c.element());
                    }
                }))
                .apply(Regex.find(Pattern.compile("/\\d+")))
                .apply(Regex.replaceFirst("/", ""))
                .apply(new HttpFuncs.CreateRequestUrsl())
                .apply(ParDo.of(HttpFuncs.getHttpRequestRunnerFunc()))
                .apply(GroupByKey.create())
                .apply(ParDo.of(HtmlProcessor.createPartProcessor(pipelineConfig.targetFolder)));

        keyCollection.apply(Distinct.create())
                .apply(Combine.globally(HtmlProcessor.createCatalogProcesssor(pipelineConfig.targetFolder)));
        pipeline.run().waitUntilFinish();
    }
}



