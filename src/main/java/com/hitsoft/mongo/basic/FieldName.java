package com.hitsoft.mongo.basic;

/**
 * User: smeagol
 * Date: 05.11.11
 * Time: 14:19
 */
public class FieldName {
    static String camelizeFieldName(Enum field) {
        String res = field.name().toLowerCase();
        if (!"_id".equals(res)) {
            assert (!res.startsWith("_") && !res.endsWith("_")) : String.format("Field name enum constant '%s' should not start or end with '_' char.", field.name());
            while (res.contains("_")) {
                String tmp = res;
                int idx = tmp.indexOf("_");
                res = tmp.substring(0, idx);
                if (tmp.length() > idx + 1) {
                    res = res + tmp.substring(idx + 1, idx + 2).toUpperCase();
                }
                if (tmp.length() > idx + 2) {
                    res = res + tmp.substring(idx + 2);
                }
            }
        }
        return res;
    }

    public static String get(Enum field) {
        return camelizeFieldName(field);
    }
}
