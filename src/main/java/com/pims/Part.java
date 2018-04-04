package com.pims;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by kshakirov on 4/2/18.
 */
public class Part implements Serializable {
    public String sku;
    public String manufacturer;
    public String name;
    public String part_type;
    public String part_number;
    public String [] turbo_model;
    public String [] turbo_type;
    public String description;
    public List<Map<String,Object>> critical;
}
