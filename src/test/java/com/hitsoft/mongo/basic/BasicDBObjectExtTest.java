package com.hitsoft.mongo.basic;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 02.11.11
 * Time: 21:56
 */
public class BasicDBObjectExtTest {

    private static enum Field {
        _ID,
        NAME,
        COMPANY,
        COMPANY_NAME,
        VERY_LONG_FIELD_NAME,
        _TEST,
        TEST_,
        _$TEST,
        $TEST
    }

    @Test
    public void testCamelizeFieldName() throws Exception {
        Assert.assertEquals("_id", BasicDBObjectExt.camelizeFieldName(Field._ID));
        Assert.assertEquals("name", BasicDBObjectExt.camelizeFieldName(Field.NAME));
        Assert.assertEquals("company", BasicDBObjectExt.camelizeFieldName(Field.COMPANY));
        Assert.assertEquals("companyName", BasicDBObjectExt.camelizeFieldName(Field.COMPANY_NAME));
        Assert.assertEquals("veryLongFieldName", BasicDBObjectExt.camelizeFieldName(Field.VERY_LONG_FIELD_NAME));
        Assert.assertEquals("test", BasicDBObjectExt.camelizeFieldName(Field._TEST));
        Assert.assertEquals("test", BasicDBObjectExt.camelizeFieldName(Field.TEST_));
        Assert.assertEquals("$test", BasicDBObjectExt.camelizeFieldName(Field._$TEST));
        Assert.assertEquals("$test", BasicDBObjectExt.camelizeFieldName(Field.$TEST));
    }
}
