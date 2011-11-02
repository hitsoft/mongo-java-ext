package com.hitsoft.mongo.basic;

import com.hitsoft.types.Currency;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.Date;

/**
 * Basic DBObjectExt implementation class
 */
public class BasicDBObjectExt extends BasicDBObject implements DBObjectExt {
    public static final int MONEY_PRECISION = 4;

    public BasicDBObjectExt() {
        super();
    }

    public BasicDBObjectExt(DBObject source) {
        super();
        putAll(source);
    }

    private static class BasicValue implements Value {
        Object val;

        private BasicValue(Object val) {
            this.val = val;
        }

        public Object val() {
            return val;
        }

        public DBObjectExt asObject() {
            DBObjectExt result = null;
            if (val instanceof DBObject)
                result = new BasicDBObjectExt((DBObject) val);
            return result;
        }

        public String asString() {
            String result = null;
            if (val != null)
                result = val.toString();
            return result;
        }

        public Date asDate() {
            Date result = null;
            if (val instanceof Date)
                result = (Date) val;
            return result;
        }

        public int asInt() {
            int result = 0;
            if (val instanceof Integer)
                result = ((Integer) val).intValue();
            else if (val instanceof Long)
                result = ((Long) val).intValue();
            return result;
        }

        public long asLong() {
            long result = 0;
            if (val instanceof Integer)
                result = ((Integer) val).longValue();
            else if (val instanceof Long)
                result = ((Long) val).longValue();
            return result;
        }

        public Currency asMoney() {
            Currency result = null;
            if (val != null) {
                result = Currency.valueOf(asLong());
            }
            return result;
        }

        public float asFloat() {
            float result = 0;
            if (val instanceof Float)
                result = ((Float) val).floatValue();
            else if (val instanceof Double)
                result = ((Double) val).floatValue();
            return result;
        }

        public double asDouble() {
            double result = 0;
            if (val instanceof Float)
                result = ((Float) val).doubleValue();
            else if (val instanceof Double)
                result = ((Double) val).doubleValue();
            return result;
        }

        public boolean isNull() {
            return val == null;
        }

        public <T extends Enum> T asEnum(Class<T> clazz) {
            T result = null;
            if (val != null)
                result = (T)Enum.valueOf(clazz, asString());
            return result;
        }
    }


    public String getId() {
        return get("_id").asString();
    }

    public void setId(String id) {
        put("_id", new ObjectId(id));
    }

    @Override
    public Value get(String key) {
        return new BasicValue(super.get(key));
    }

    public Value get(Enum field) throws WrongFieldName {
        return get(camelizeFieldName(field));
    }

    @Override
    public Object put(String field, Object value) {
        Object val = value;
        if (value instanceof Currency) {
            val = ((Currency) value).longValue();
        } else if (value instanceof Enum) {
            val = ((Enum) value).name();
        } else if (value instanceof DBObjectBuilder)
            val = ((DBObjectBuilder) value).get();
        return super.put(field, val);
    }

    public Object put(Enum field, Object value) throws WrongFieldName {
        return put(camelizeFieldName(field), value);
    }

    static String camelizeFieldName(Enum field) throws WrongFieldName {
        String res = field.name().toLowerCase();
        if (!"_id".equals(res)) {
            if (res.startsWith("_"))
                throw new WrongFieldName(field);
            if (res.endsWith("_"))
                throw new WrongFieldName(field);
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

}
