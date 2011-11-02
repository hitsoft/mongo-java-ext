package com.hitsoft.mongo.basic;

import com.hitsoft.types.Currency;
import com.mongodb.DBObject;

import java.util.Date;

/**
 * Extended DBObject. Utilizes another typing idea than original one.
 * Also adding possibility to use Enums for field names and working with Ids as Strings;
 */
public interface DBObjectExt extends DBObject {

    public static interface Value {
        DBObjectExt asObject();
        String asString();
        Date asDate();
        int asInt();
        long asLong();
        Currency asMoney();
        float asFloat();
        double asDouble();
        boolean isNull();
        <T extends Enum> T asEnum(Class<T> clazz);
    }

    public static class WrongFieldName extends Throwable {
        public Enum field;
        public WrongFieldName(Enum field) {
            super(String.format("Wrong field name: '%s'. Field names can not start or end with '_' char, except reserved field '_id'", field.name()));
            this.field = field;
        }
    }

    String getId();
    void setId(String id);

    Value get(String field);
    Value get(Enum field) throws WrongFieldName;

    Object put(String field, Object value);
    Object put(Enum field, Object value) throws WrongFieldName;
}
