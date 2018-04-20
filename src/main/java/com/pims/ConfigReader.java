package com.pims;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by kshakirov on 4/20/18.
 */
public class ConfigReader {
    private static String configFileName = "config.json";
    private static String readConfig(String filename) throws IOException {
        return  FileUtils.readFileToString(new File(filename),"UTF-8");
    }

    public static PipelineConfig  getConfig(){
        Gson gson = new Gson();
        try {
            return  gson.fromJson(readConfig(configFileName),PipelineConfig.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
