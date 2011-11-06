package com.hitsoft.mongo.basic;

import junit.framework.Assert;
import org.junit.Test;

/**
 * User: smeagol
 * Date: 05.11.11
 * Time: 14:13
 */
public class ConnectionTest {

    @Test
    public void testGetInstance() throws Exception {
        boolean assertion = false;
        try {
            Connection.getInstance();
        } catch (AssertionError e) {
            assertion = true;
        }
        Assert.assertTrue(assertion);

        Connection.setupTestInstance();
        assertion = false;
        try {
            Connection.getInstance();
        } catch (AssertionError e) {
            assertion = true;
        }
        Assert.assertFalse(assertion);
    }
}
