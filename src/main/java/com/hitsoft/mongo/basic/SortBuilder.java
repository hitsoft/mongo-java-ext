package com.hitsoft.mongo.basic;

/**
 * User: smeagol
 * Date: 03.11.11
 * Time: 22:07
 */
public class SortBuilder extends BaseBuilder {

    public static SortBuilder start() {
        return new SortBuilder();
    }

    public SortBuilder asc(Enum field) throws DBObjectExt.WrongFieldName {
        obj.add(field, 1);
        return this;
    }

    public SortBuilder desc(Enum field) throws DBObjectExt.WrongFieldName {
        obj.add(field, -1);
        return this;
    }

}
