package com.hitsoft.mongo.basic;

/**
 *
 */
public class ShowFieldsBuilder extends BaseBuilder {

    public static ShowFieldsBuilder start() {
        return new ShowFieldsBuilder();
    }

    public ShowFieldsBuilder add(Enum field) throws DBObjectExt.WrongFieldName {
        obj.add(field, 1);
        return this;
    }

}
