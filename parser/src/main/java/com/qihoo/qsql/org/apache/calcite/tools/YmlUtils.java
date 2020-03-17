package com.qihoo.qsql.org.apache.calcite.tools;


import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * @Description
 * @Date 2020/3/12 12:01
 * @Created by fangyue1
 */
public class YmlUtils {
    private static final Yaml yaml = new Yaml();
    private static Map<String, Map<String, String>> sourceMap = (Map<String, Map<String, String>>) yaml
        .load(YmlUtils.class.getClassLoader().getResourceAsStream("jdbc_source.yml"));

    public static Map<String, Map<String, String>> getSourceMap() {
        if (sourceMap == null || sourceMap.size() == 0) {
            throw new RuntimeException("source params is empty ");
        }
        return sourceMap;
    }
}
