package com.hitsoft.mongo.managed;

import org.bson.types.ObjectId;

/**
 * Managed object base class to store in MongoDB.
 * Managed objects can be marshalled/demarshalled to/from Mongo database automatically.
 */
public class ManagedObject {

    public static enum Field {
        _ID
    }

    @MongoId
    ObjectId _id;

    public ObjectId id() {
        return _id;
    }

    protected ManagedObject() {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Class clazz = getClass();
        sb.append(clazz.getSimpleName())
                .append("{")
                .append("_id=")
                .append(_id);
        for (java.lang.reflect.Field field : clazz.getFields()) {
            sb.append(", ");
            sb.append(field.getName()).append("=");
            Object val = null;
            try {
                val = field.get(this);
            } catch (IllegalAccessException ignored) {
            }
            if (val instanceof String) {
                sb.append("'").append(val).append("'");
            } else {
                sb.append(val);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        boolean result = true;
        if (this != o) {
            Class clazz = this.getClass();
            result = (o != null && clazz.equals(o.getClass()));
            result = (result
                    && ((this._id == null && ((ManagedObject) o)._id == null)
                    || (this._id != null && ((ManagedObject) o)._id != null)
                    && this._id.equals(((ManagedObject) o)._id)));
            if (result) {
                for (java.lang.reflect.Field field : clazz.getFields()) {
                    if (result) {
                        try {
                            Object val = field.get(this);
                            Object oVal = field.get(o);
                            if (val == null) {
                                result = (oVal == null);
                            } else {
                                result = val.equals(oVal);
                            }
                        } catch (IllegalAccessException ignored) {
                            result = false;
                        }
                    }
                }
            }
        }
        return result;
    }
}
