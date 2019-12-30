package com.qihoo.qsql.exec;

import java.util.HashMap;
import java.util.Map;

public class DdlFactory {
    static Map<String, DdlOperation> ddlMap = new HashMap<>();

    static {
        ddlMap.put("SHOW", new ShowDbHandler());
        ddlMap.put("DESCRIBE", new ShowDbHandler());
    }

    /**
     * get ddl handler according sql type.
     * @param sqlType sql type.
     * @return
     */
    public static DdlOperation getDdlOperation(String sqlType) {
        return ddlMap.get(sqlType);
    }
}
