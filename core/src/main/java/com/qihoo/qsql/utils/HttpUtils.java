package com.qihoo.qsql.utils;

import com.google.common.io.CharStreams;
import com.google.gson.JsonObject;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;

public class HttpUtils {

    /**
     * parse url.
     */
    public static String parseUrlArgs(String responseUrl,JsonObject jsonObject) {
        String[] split1 = responseUrl.split("\\?");
        String url = split1[0];
        if (split1.length > 1) {
            String[] split = split1[1].split("&");
            for (int i = 0;i < split.length; i ++) {
                String[] args = split[i].split("=");
                jsonObject.addProperty(args[0],args[1]);
            }
        }
        return url;
    }

    /**
     * send post request.
     */
    public static String post(String httpUrl, String content) throws Exception {
        Map<String, String> headers = new HashMap<>(1);
        headers.put("Content-type", "application/json");
        URL url = new URL(httpUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        if (null != headers && !headers.isEmpty()) {
            for (Entry<String, String> entry : headers.entrySet()) {
                conn.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        if (StringUtils.isNotBlank(content)) {
            conn.getOutputStream().write(content.getBytes(StandardCharsets.UTF_8));
        }
        conn.connect();
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("request failure, status code:" + conn.getResponseCode());
        }
        String result = CharStreams
            .toString(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
        conn.disconnect();
        return result;
    }
}
