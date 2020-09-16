package com.qihoo.qsql.org.apache.calcite.tools;


import java.util.Map;
import org.yaml.snakeyaml.Yaml;

public class JdbcSourceInfo {
    private static final Yaml yaml = new Yaml();
    private static Map<String, Map<String, String>> sourceMap = (Map<String, Map<String, String>>) yaml
        .load(JdbcSourceInfo.class.getClassLoader().getResourceAsStream("jdbc_source.yml"));

    public static Map<String, Map<String, String>> getSourceMap() {
        if (sourceMap == null || sourceMap.size() == 0) {
            throw new RuntimeException("source params is empty ");
        }
        return sourceMap;
    }
}
