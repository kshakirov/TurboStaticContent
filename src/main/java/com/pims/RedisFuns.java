package com.pims;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by kshakirov on 3/30/18.
 */
public class RedisFuns {
    static class CreteRedisKeyValue extends DoFn<KV<String, String>, KV<String, String>> {

    }
}
