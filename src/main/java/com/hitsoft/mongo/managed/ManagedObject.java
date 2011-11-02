package com.hitsoft.mongo.managed;

/**
 * Managed object base class to store in MongoDB.
 * Managed objects can be marshalled/demarshalled to/from Mongo database automatically.
 */
public class ManagedObject {

    public static enum Field {
        _ID;
    }

    @MongoId
    private String _id;

    public String id() {
        return _id;
    }

    protected ManagedObject() {
    }

    @Override
    public String toString() {
        return "ManagedObject{" + "_id='" + _id + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ManagedObject)) return false;

        ManagedObject that = (ManagedObject) o;

        //noinspection RedundantIfStatement
        if (_id != null ? !_id.equals(that._id) : that._id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return _id != null ? _id.hashCode() : 0;
    }

}