package com.hitsoft.test;

import org.junit.Assert;

import java.util.List;

/**
 * User: smeagol
 * Date: 06.11.11
 * Time: 12:45
 */
public class AssertExt {

    public static void assertEquals(Object[] expected, Object[] actual) {
        Assert.assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], actual[i]);
        }
    }

    public static void assertEquals(Object[] expected, List actual) {
        assertEquals(expected, actual.toArray());
    }
}
