package com.qihoo.qsql.metadata.extern;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Metadata option parser Class.
 * <p>
 * This class tries to parsing options coming from command line. Now qsql support two options. One is dbType, which aims
 * to decide storage type for extern metadata storage, such as mysql. The other is action, and it decides which action
 * needs to be done with metadata, such as init and delete.
 * </p>
 */
public class MetadataOptionParser {

    //calculation engine related params
    private static final String DB_TYPE = "--dbType";
    private static final List<String> DB_TYPE_VALUES = Arrays.asList("mysql", "oracle");
    private static final String ACTION = "--action";
    private static final List<String> ACTION_VALUES = Arrays.asList("init", "delete", "initAndUpdate");

    private final String[][] metadataOptions = {
        {DB_TYPE},
        {ACTION}
    };
    private Properties properties = new Properties();

    /**
     * parse args from Bash Command.
     *
     * @param args args from Bash Command
     */
    public final void parse(List<String> args) {
        Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.*)");

        int idx = 0;
        for (idx = 0; idx < args.size(); idx++) {
            String arg = args.get(idx);
            String value = null;

            Matcher matcher = eqSeparatedOpt.matcher(arg);
            if (matcher.matches()) {
                arg = matcher.group(1);
                value = matcher.group(2);
            }

            parseMetadataOptions(arg, value);
        }
    }

    public Properties getProperties() {
        return properties;
    }

    private void parseMetadataOptions(String arg, String value) {
        String name = findCliOption(arg, metadataOptions);
        if (name != null) {
            String checkedValue = checkNameAndValue(name, value);
            properties.setProperty(name, checkedValue);
        }
    }

    private String findCliOption(String name, String[][] available) {
        for (String[] candidates : available) {
            for (String candidate : candidates) {
                if (candidate.equals(name)) {
                    return candidates[0];
                }
            }
        }
        return null;
    }

    private String checkNameAndValue(String name, String value) {
        switch (name) {
            case DB_TYPE:
                if (! DB_TYPE_VALUES.contains(value)) {
                    throw new RuntimeException("Qsql cannot support this type of metadata: " + value);
                }
                return value;
            case ACTION:
                if (! ACTION_VALUES.contains(value)) {
                    throw new RuntimeException("Qsql cannot support this type of action: " + value);
                }
                return value;
            default:
                throw new RuntimeException("Qsql cannot support this param: " + name);

        }
    }

}
