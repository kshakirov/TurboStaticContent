package com.pims;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by kshakirov on 2/23/18.
 */
public class HttpFuncs {
    static class HttpRequestRunner extends DoFn<KV<String, String>, KV<String, KV<String, String>>> {
        private String host = "http://staging.turbointernational.com/";

        private String getType(String url) {
            String[] segments = url.split("/");
            return segments[6];
        }

        private String makeGetRequest(String url, String sku) {
            HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(url);
            String type = getType(url);
            String content = "{}";
            try {
                HttpResponse response = client.execute(request);
                content = IOUtils.toString(response.getEntity().getContent());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return content;

        }

        private String makePostRequest(String url, String sku) {

            CloseableHttpClient client = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            String json = "{\"sku\": " + sku + "}";
            StringEntity entity = null;
            try {
                entity = new StringEntity(json);

            } catch (UnsupportedEncodingException e) {

                e.printStackTrace();
            }
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");
            ResponseHandler<String> handler = new BasicResponseHandler();
            CloseableHttpResponse response = null;
            String body = "{}";
            try {
                response = client.execute(httpPost);
                body = handler.handleResponse(response);
                System.out.println(String.format("Received Part [%s]", sku));
                client.close();
            } catch (IOException e) {
                System.out.println(String.format("Cannot Get Part [%s]", sku));
                System.out.println(e.getMessage());
            }
            return body;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            String url = host + c.element().getValue();
            String sku = ((KV<String, String>) c.element()).getKey();
            String subKey = "part";
            url = url.replace("SKU", sku);
            String body = "";
            if (url.contains("frontend")) {
                body = makePostRequest(url, sku);
            } else if (url.contains("where_used")) {
                body = makePostRequest(url, sku);
                subKey = "where_used";
            } else {
                body = makeGetRequest(url, sku);
                subKey = String.format("%s_%s", sku, getType(url));
                subKey = "interchanges";
            }
            c.output(KV.of(sku, KV.of(subKey, body)));


        }
    }

    static class CreateRequestUrsl extends PTransform<PCollection<String>,
            PCollection<KV<String, String>>> {
        @Override
        public PCollection<KV<String, String>> expand(PCollection<String> sku) {
            PCollection<KV<String, String>> urls =
                    sku.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            ArrayList<String> urls = new ArrayList<String>(Arrays.asList(
                                    "frontend/product", "attrsreader/product/SKU/interchanges/",
                                    "attrsreader/product/SKU/where_used/"));
                            for (String url : urls
                                    ) {
                                c.output(KV.of(c.element(), url));
                            }


                        }
                    }));

            return urls;
        }
    }

    public static HttpRequestRunner getHttpRequestRunnerFunc() {
        return new HttpRequestRunner();
    }

    public static CreateRequestUrsl getCreateRequestUrlsFunc() {
        return new CreateRequestUrsl();
    }
}
