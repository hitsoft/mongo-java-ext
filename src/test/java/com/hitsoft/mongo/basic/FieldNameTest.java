package com.hitsoft.mongo.basic;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: smeagol
 * Date: 05.11.11
 * Time: 14:22
 */
public class FieldNameTest {
    private static enum Field {
        _ID,
        NAME,
        COMPANY,
        COMPANY_NAME,
        VERY_LONG_FIELD_NAME,
        _TEST,
        TEST_,
        $TEST
    }

    @Test
    public void testCamelizeFieldName() throws Exception {
        Assert.assertEquals("_id", FieldName.camelizeFieldName(Field._ID));
        Assert.assertEquals("name", FieldName.camelizeFieldName(Field.NAME));
        Assert.assertEquals("company", FieldName.camelizeFieldName(Field.COMPANY));
        Assert.assertEquals("companyName", FieldName.camelizeFieldName(Field.COMPANY_NAME));
        Assert.assertEquals("veryLongFieldName", FieldName.camelizeFieldName(Field.VERY_LONG_FIELD_NAME));

        boolean assertion = false;
        try {
            FieldName.camelizeFieldName(Field._TEST);
        } catch (AssertionError e) {
            assertion = true;
        }
        Assert.assertTrue(assertion);

        assertion = false;
        try {
            FieldName.camelizeFieldName(Field.TEST_);
        } catch (AssertionError e) {
            assertion = true;
        }
        Assert.assertTrue(assertion);

        Assert.assertEquals("$test", FieldName.camelizeFieldName(Field.$TEST));
    }

}
