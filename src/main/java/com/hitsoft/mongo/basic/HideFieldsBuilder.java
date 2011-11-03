package com.hitsoft.mongo.basic;

/**
 * User: smeagol
 * Date: 03.11.11
 * Time: 22:34
 */
public class HideFieldsBuilder extends BaseBuilder {

    public static HideFieldsBuilder start() {
        return new HideFieldsBuilder();
    }

    public HideFieldsBuilder add(Enum field) {
        obj.add(field, 0);
        return this;
    }

}
