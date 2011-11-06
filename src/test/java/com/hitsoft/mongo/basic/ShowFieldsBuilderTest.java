package com.hitsoft.mongo.basic;

import org.junit.Assert;
import org.junit.Test;

/**
 * User: smeagol
 * Date: 05.11.11
 * Time: 14:30
 */
public class ShowFieldsBuilderTest {

    private static enum Field {
        TEST
    }

    @Test
    public void testAdd() throws Exception {
        ShowFieldsBuilder test = ShowFieldsBuilder.start();
        test.add(Field.TEST);
        Assert.assertEquals(1, test.get().toMap().size());
        Assert.assertFalse(test.getExt().getV(Field.TEST).isNull());
        Assert.assertEquals(1, test.getExt().getV(Field.TEST).asLong());
    }
}
