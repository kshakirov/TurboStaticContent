package com.pims;

/**
 * Created by kshakirov on 4/2/18.
 */

import avro.shaded.com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.neuland.jade4j.Jade4J;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.neuland.jade4j.Jade4J.getTemplate;


public class HtmlProcessor {
    static class CatalogProcessor implements SerializableFunction<Iterable<String>, String> {
        private File catalogFile;

        CatalogProcessor(String targetFolder) {
            catalogFile = new File(targetFolder + "catalog.html");
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


        private String createSnippet(){
            return "{\n" +
                    "  \"@context\": \"http://schema.org\",\n" +
                    "  \"@type\": \"Organization\",\n" +
                    "  \"url\": \"https://www.turbointernational.com\",\n" +
                    "  \"name\": \"Turbo International\",\n" +
                    "  \"description\":\"Turbo International was founded in 1989 and since then has been dedicated to serving the global turbocharger aftermarket with an ever increasing range of high quality turbocharger assemblies and component parts at competitive prices. In addition to our headquarters and inventory located in southern California, we have a European sales office based in the UK. From these locations we serve customers on six continents. \",\n" +
                    "  \"contactPoint\": [\n" +
                    "    {\n" +
                    "      \"@type\": \"ContactPoint\",\n" +
                    "      \"telephone\": \"+1-877-746-0909\",\n" +
                    "      \"contactType\": \"Customer service\",\n" +
                    "      \"areaServed\": \"US\"\n" +
                    "    }\n" +
                    "  ]\n" +
                    "}";
        }

        private Map<String, Object> createModel(List<String> keys) {
            Map<String, Object> model = new HashMap<>();
            model.put("parts", createCatalogTable(keys));
            model.put("snippet", createSnippet());
            return model;
        }

        @Override
        public String apply(Iterable<String> keys) {
            if (keys.iterator().hasNext()) {
                List<String> keysList = Lists.newArrayList(keys);
                Map<String, Object> model = createModel(keysList);
                if (keysList.size() > 1) {
                    try {
                        String html = Jade4J.render("templates/catalog_template.jade", model);
                        System.out.println(String.format("Appending to File [%d] items", keysList.size()));
                        FileUtils.writeStringToFile(catalogFile, html, true);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return "";

        }
    }

    static class PartProcessor extends DoFn<KV<String, Iterable<KV<String, String>>>, String> {
        private String folder;
        PartProcessor(String targetFolder){
            folder = targetFolder;
        }

        private String getTemplate(String filename) {
            return getClass().getClassLoader().getResource("templates/" + filename).getFile();

        }

        private String createPageTitle(Part part) {
            return String.format("%s %s %s", part.manufacturer, part.part_type, part.name);
        }

        private String createContent(Part part) {
            return String.format("Product: %s, Category: %s , Manufacturer: %s", part.name, part.part_type, part.manufacturer);
        }

        private String createSnippet(Part part){
            return String.format("{\n" +
                    "\"@context\": \"http://schema.org\",\n" +
                    "\"@type\": \"Product\",\n" +
                    "\"category\": \"%s\",\n" +
                    "\"name\": \"%s\",\n" +
                    "\"ProductId\": \"%s\",\n" +
                    "\"manufacturer\": \"%s\",\n" +
                    "\"sku\": \"%s\",\n" +
                    "\"url\": \"https://www.turbointernational.com/part/sku/%s\"\n" +
                    "}\n", part.part_type, part.name, part.part_number,
                    part.manufacturer, part.part_number, part.sku);
        }

        private Map<String, Object> createProductInformation(Part part) {
            Map<String, Object> model = new HashMap<String, Object>();
            model.put("pageTitle", createPageTitle(part));
            model.put("content", createContent(part));
            model.put("snippet", createSnippet(part));
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

        private Map<String, Object> createPartModel(String partSource, String sku) {
            Gson gson = new Gson();
            Part part = gson.fromJson(partSource, Part.class);
            return createModel(part);

        }

        private List<Map<String, Object>> createInterchanges(Interchange[] interchanges) {
            Stream<Interchange> stream = Stream.of(interchanges);
            return stream.map(i -> {
                Map<String, Object> ii = new HashedMap();
                ii.put("manufacturer", i.manufacturer);
                ii.put("part_number", i.part_number);
                ii.put("description", i.description);
                return ii;
            }).collect(Collectors.toList());
        }

        private List<Map<String, Object>> createWhereUseds(Collection<WhereUsed> whereUseds) {

            return whereUseds.stream().map(w -> {
                Map<String, Object> ii = new HashedMap();
                ii.put("manufacturer", w.manufacturer);
                ii.put("partNumber", w.partNumber);
                ii.put("tiPartNumber", w.tiPartNumber);
                Stream stream = Stream.of(w.turboPartNumbers);
                String numbers = stream.collect(Collectors.joining(",")).toString();
                ii.put("turboPartNumbers", numbers);
                return ii;
            }).collect(Collectors.toList());
        }

        private Map<String, Object> addInterchangesModel(Map<String, Object> model, String interchanges) {
            Gson gson = new Gson();
            Interchange[] ints = gson.fromJson(interchanges, Interchange[].class);
            if (ints.length > 0)
                model.put("interchanges", createInterchanges(ints));
            else
                model.put("interchanges", Collections.emptyList());
            return model;
        }

        private Map<String, Object> addWhereUsedModel(Map<String, Object> model, String whereuseds) {
            Gson gson = new Gson();
            Type WhereUsedMap = new TypeToken<HashMap<String, WhereUsed>>() {
            }.getType();

            Map<String, WhereUsed> wusds = gson.fromJson(whereuseds, WhereUsedMap);
            Collection<WhereUsed> whereUseds = wusds.values();
            if (whereUseds.size() > 0)
                model.put("where_useds", createWhereUseds(whereUseds));
            else
                model.put("where_useds", Collections.emptyList());
            return model;
        }

        private String createPartPage(String key, Iterator<KV<String, String>> iterator) {
            Map<String, Object> model = null;
            String html = "";
            while (iterator.hasNext()) {
                KV<String, String> elem = iterator.next();
                if (elem.getKey().equalsIgnoreCase("part"))
                    model = createPartModel(elem.getValue(), key);
                if (elem.getKey().equalsIgnoreCase("interchanges"))
                    model = addInterchangesModel(model, elem.getValue());
                if (elem.getKey().equalsIgnoreCase("where_used"))
                    model = addWhereUsedModel(model, elem.getValue());
            }
            try {
                html = Jade4J.render("templates/part_template.jade", model);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return html;

        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            File temp = new File(folder + c.element().getKey() + ".html");
            try {
                Iterator<KV<String, String>> iterator = c.element().getValue().iterator();
                String key = c.element().getKey();
                String html = createPartPage(key, iterator);
                FileUtils.writeStringToFile(temp, html);
                List<PCollection<String>> sourceStringCollection = new ArrayList<>();
                c.output(key);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static PartProcessor createPartProcessor(String targetFolder) {

        return new PartProcessor(targetFolder);
    }

    public static CatalogProcessor createCatalogProcesssor(String targetFolder) {
        return new CatalogProcessor(targetFolder);
    }

}


