package com.pims.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Created by kshakirov on 2/26/18.
 */

public interface CustomOptions extends PipelineOptions {
    @Description("My custom command line argument.")
    @Default.String("DEFAULT")
    String getMyCustomOption();
    void setMyCustomOption(String myCustomOption);
}