package com.hitsoft.mongo.basic;

/**
 *
 */
public class ColumnsBuilder {

    private DBObjectBuilder obj = DBObjectBuilder.start();

    private ColumnsBuilder() {
    }

    public static ColumnsBuilder start() {
        return new ColumnsBuilder();
    }

    public ColumnsBuilder add(Enum field) throws DBObjectExt.WrongFieldName {
        obj.add(field, 1);
        return this;
    }

    public DBObjectExt get() {
        return obj.get();
    }
}
