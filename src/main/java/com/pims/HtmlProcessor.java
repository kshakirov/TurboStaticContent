package com.pims;

/**
 * Created by kshakirov on 4/2/18.
 */

import avro.shaded.com.google.common.collect.Lists;
import com.google.gson.Gson;
import de.neuland.jade4j.Jade4J;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.neuland.jade4j.Jade4J.getTemplate;


public class HtmlProcessor {
    static class CatalogProcessor implements SerializableFunction<Iterable<String>, String> {
        private File catalogFile;

        CatalogProcessor() {
            catalogFile = new File("responses/catalog.html");
            catalogFile.delete();
            try {
                System.out.println("Creating new file");
                catalogFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        private String getTemplate(String filename) {
            return getClass().getClassLoader().getResource("templates/" + filename).getPath();

        }

        private List<Map<String, String>> createCatalogTable(List<String> keys) {
            return keys.stream().map(k -> {
                Map<String, String> part = new HashMap<>();
                part.put("sku", k);
                part.put("url", "/part/sku/" + k);
                return part;
            }).collect(Collectors.toList());
        }

        private Map<String, Object> createModel(List<String> keys) {
            Map<String, Object> model = new HashMap<>();
            model.put("parts",createCatalogTable(keys));
            return model;
        }

        @Override
        public String apply(Iterable<String> keys) {
            if(keys.iterator().hasNext()) {
                List<String> keysList = Lists.newArrayList(keys);
                Map<String, Object> model = createModel(keysList);
                if(keysList.size() > 1) {
                    try {
                        String html = Jade4J.render(getTemplate("catalog_template.jade"), model);
                        System.out.println(String.format("Appending to File [%d] items",keysList.size()));
                        FileUtils.writeStringToFile(catalogFile, html,true);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return "";

        }
    }

    static class PartProcessor extends DoFn<KV<String, Iterable<KV<String, String>>>, String> {

        private String getTemplate(String filename) {
            return getClass().getClassLoader().getResource("templates/" + filename).getPath();

        }

        private Map<String, Object> createProductInformation(Part part) {
            Map<String, Object> model = new HashMap<String, Object>();
            model.put("pageTitle", part.name);
            model.put("manufacturer", part.manufacturer);
            model.put("part_number", part.part_number);
            model.put("part_type", part.part_type);
            model.put("description", part.description);
            return model;
        }

        private Map<String, Object> createAdditionalInformation(Map<String, Object> model, Part part) {
            if (part.turbo_type != null) {
                Stream<String> turbo_type = Stream.of(part.turbo_type);
                model.put("turbo_model", turbo_type.collect(Collectors.joining(", ")));

            }
            if (part.turbo_model != null) {
                Stream<String> turbo_model = Stream.of(part.turbo_model);
                model.put("turbo_type", turbo_model.collect(Collectors.joining(", ")));
            }
            return model;
        }

        private Map<String, Object> createCriticalItem(Map<String, Object> item) {
            Map<String, Object> row = new HashedMap();
            row.put("label", item.get("label"));
            if ((boolean) item.get("decimal") && (item.get("value") != null)) {
                Map<String, Double> v = (Map<String, Double>) item.get("value");
                row.put("value", v.get("inches"));
            } else {
                row.put("value", item.get("value"));
            }

            return row;
        }

        private Map<String, Object> createCriticalInformation(Map<String, Object> model, Part part) {
            if (part.critical != null) {
                List<Map<String, Object>> rows = part.critical.stream().map(m -> {
                    Map<String, Object> row = createCriticalItem(m);
                    return row;
                }).collect(Collectors.toList());
                model.put("rows", rows);
            } else {
                model.put("rows", Collections.emptyList());
            }
            return model;
        }

        private Map<String, Object> createModel(Part part) {
            Map<String, Object> model = createProductInformation(part);
            model = createAdditionalInformation(model, part);
            model = createCriticalInformation(model, part);
            return model;
        }

        private String createPartModel(String partSource, String sku) {
            Gson gson = new Gson();
            Part part = gson.fromJson(partSource, Part.class);
            String html = "";
            Map<String, Object> model = createModel(part);
            try {
                html = Jade4J.render(getTemplate("part_template.jade"), model);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return html;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            File temp = new File("responses/" + c.element().getKey() + ".html");
            try {
                Iterator<KV<String, String>> iterator = c.element().getValue().iterator();
                StringBuilder stringBuilder = new StringBuilder();
                String html = "empty";
                while (iterator.hasNext()) {
                    KV<String, String> elem = iterator.next();
                    if (elem.getKey().equalsIgnoreCase("part"))
                        stringBuilder.append(elem.getValue());
                    html = createPartModel(stringBuilder.toString(), c.element().getKey());
                }
                FileUtils.writeStringToFile(temp, html);
                List<PCollection<String>> sourceStringCollection = new ArrayList<>();
                String key = c.element().getKey();
                c.output(key);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static PartProcessor createPartProcessor() {
        return new PartProcessor();
    }

    public static CatalogProcessor createCatalogProcesssor() {
        return new CatalogProcessor();
    }

}


