package com.hitsoft.mongo.basic;

import com.hitsoft.types.Currency;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Basic DBObjectExt implementation class
 */
public class BasicDBObjectExt extends BasicDBObject implements DBObjectExt {

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

        @Override
        public ObjectId asId() {
            return (ObjectId) val;
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
                result = (Integer) val;
            else if (val instanceof Long)
                result = ((Long) val).intValue();
            else if (val != null) {
                try {
                    result = Integer.parseInt(val.toString());
                } catch (NumberFormatException e) {
                    result = 0;
                }
            }
            return result;
        }

        public long asLong() {
            long result = 0;
            if (val instanceof Integer)
                result = ((Integer) val).longValue();
            else if (val instanceof Long)
                result = (Long) val;
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
                result = (Float) val;
            else if (val instanceof Double)
                result = ((Double) val).floatValue();
            return result;
        }

        public double asDouble() {
            double result = 0;
            if (val instanceof Float)
                result = ((Float) val).doubleValue();
            else if (val instanceof Double)
                result = (Double) val;
            return result;
        }

        @Override
        public boolean asBoolean() {
            boolean result = false;
            if (val instanceof Boolean)
                result = ((Boolean) val).booleanValue();
            return result;
        }

        public boolean isNull() {
            return val == null;
        }

        public <T extends Enum> T asEnum(Class<T> clazz) {
            T result = null;
            if (val != null)
                //noinspection RedundantCast
                result = (T) Enum.valueOf(clazz, asString());
            return result;
        }

        public <T> List<T> asList(Class<T> clazz) {
            List<T> result;
            if (val instanceof List) {
                if (clazz.isAssignableFrom(DBObjectExt.class)) {
                    result = new ArrayList<T>();
                    for (Object item : (List) val()) {
                        if (item instanceof DBObject) {
                            result.add((T) new BasicDBObjectExt((DBObject) item));
                        }
                    }
                } else
                    result = (List<T>) val();
            } else
                result = null;
            return result;
        }

        @Override
        public Integer asInteger() {
            return (Integer) val;
        }
    }


    public String getId() {
        return getV(DBObjectExt.Field._ID).asString();
    }

    public void setId(String id) {
        put(DBObjectExt.Field._ID, new ObjectId(id));
    }

    public Value getV(String key) {
        return new BasicValue(super.get(key));
    }

    public Value getV(Enum field) {
        return getV(FieldName.get(field));
    }

    public Value getV(Enum[] field) {
        return getV(FieldName.get(field));
    }

    public Object put(String field, Object value) {
        Object val = value;
        if (value instanceof Currency) {
            val = ((Currency) value).longValue();
        } else if (value instanceof Enum) {
            val = ((Enum) value).name();
        } else if (value instanceof DBObjectBuilder) {
            val = ((DBObjectBuilder) value).get();
        } else if (value instanceof BaseBuilder)
            val = ((BaseBuilder) value).get();
        return super.put(field, val);
    }

    public Object put(Enum field, Object value) {
        return put(FieldName.get(field), value);
    }

    @Override
    public Object put(Enum[] field, Object value) {
        return put(FieldName.get(field), value);
    }

    public DBObject asDBObject() {
        return this;
    }

}
