package com.hitsoft.mongo.basic;

import com.hitsoft.types.Currency;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

/**
 * Extended DBObject. Utilizes another typing idea than original one.
 * Also adding possibility to use Enums for field names and working with Ids as Strings;
 */
public interface DBObjectExt {

    public static enum Field {
        _ID
    }

    public static interface Value {
        Object val();

        ObjectId asId();

        DBObjectExt asObject();

        String asString();

        Date asDate();

        int asInt();

        long asLong();

        Currency asMoney();

        float asFloat();

        double asDouble();

        boolean asBoolean();

        boolean isNull();

        <T extends Enum> T asEnum(Class<T> clazz);

        <T> List<T> asList(Class<T> clazz);
    }

    ObjectId getId();

    void setId(ObjectId id);

    Value getV(String field);

    Value getV(Enum field);

    Object put(String field, Object value);

    Object put(Enum field, Object value);

    DBObject asDBObject();
}
