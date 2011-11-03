package com.hitsoft.mongo.basic;

/**
 *
 */
public class ColumnsBuilder extends BaseBuilder {

    public static ColumnsBuilder start() {
        return new ColumnsBuilder();
    }

    public ColumnsBuilder add(Enum field) throws DBObjectExt.WrongFieldName {
        obj.add(field, 1);
        return this;
    }

}
