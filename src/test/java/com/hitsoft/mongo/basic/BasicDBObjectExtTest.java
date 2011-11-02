package com.hitsoft.mongo.basic;

import com.hitsoft.mongo.basic.BasicDBObjectExt;
import com.hitsoft.mongo.basic.DBObjectExt;
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
        TEST_;
    }

    @Test
    public void testCamelizeFieldName() throws Exception {
        try {
            Assert.assertEquals("_id", BasicDBObjectExt.camelizeFieldName(Field._ID));
            Assert.assertEquals("name", BasicDBObjectExt.camelizeFieldName(Field.NAME));
            Assert.assertEquals("company", BasicDBObjectExt.camelizeFieldName(Field.COMPANY));
            Assert.assertEquals("companyName", BasicDBObjectExt.camelizeFieldName(Field.COMPANY_NAME));
            Assert.assertEquals("veryLongFieldName", BasicDBObjectExt.camelizeFieldName(Field.VERY_LONG_FIELD_NAME));
        } catch (DBObjectExt.WrongFieldName wrongFieldName) {
            Assert.fail(wrongFieldName.getMessage());
        }
        boolean error = false;
        try {
            BasicDBObjectExt.camelizeFieldName(Field._TEST);
        } catch (DBObjectExt.WrongFieldName wrongFieldName) {
            error = true;
        }
        Assert.assertTrue(error);
        error = false;
        try {
            BasicDBObjectExt.camelizeFieldName(Field.TEST_);
        } catch (DBObjectExt.WrongFieldName wrongFieldName) {
            error = true;
        }
        Assert.assertTrue(error);
    }
}
