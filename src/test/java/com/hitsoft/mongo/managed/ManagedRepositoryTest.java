package com.hitsoft.mongo.managed;

import com.hitsoft.mongo.basic.DBObjectBuilder;
import com.hitsoft.mongo.basic.DBObjectExt;
import com.hitsoft.test.AssertExt;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: smeagol
 * Date: 06.11.11
 * Time: 18:50
 */
public class ManagedRepositoryTest {

    public static enum TestEnum {
        TEST,
        VALUE
    }

    public static class TestObj extends ManagedObject {

        public static enum Field {
            ENUM_VALUE,
            STRING_VALUE,
            INT_VALUE,
            DATE_VALUE,
            STRING_LIST_VALUE,
            INNER_OBJECT_VALUE,
            INNER_OBJECT_LIST_VALUE
        }

        public static class InnerObject extends ManagedObject {

            public static enum Field {
                NAME,
                VALUE
            }

            public String name;
            public int value;
        }

        @EnumField(type = TestEnum.class)
        public TestEnum enumValue;
        public String stringValue;
        public int intValue;
        public Date dateValue;
        @ListField(type = String.class)
        public List<String> stringListValue;
        public TestObj.InnerObject innerObjectValue;
        @ListField(type = TestObj.InnerObject.class)
        public List<TestObj.InnerObject> innerObjectListValue;
    }

    private static class TestNotAnnotatedEnum extends ManagedObject {
        @EnumField(type = TestEnum.class)
        public TestEnum annotated = TestEnum.TEST;
        public TestEnum notAnnotated = TestEnum.VALUE;
    }

    private static class TestNotAnnotatedList extends ManagedObject {
        @ListField(type = TestObj.InnerObject.class)
        public List<TestObj.InnerObject> annotated = new ArrayList<TestObj.InnerObject>();
        public List<TestObj.InnerObject> notAnnotated = new ArrayList<TestObj.InnerObject>();
    }

    private static class TestNotAnnotatedRef extends ManagedObject {
        @MongoRef(ref = TestObj.class)
        public ObjectId annotated = new ObjectId();
        public ObjectId notAnnotated = new ObjectId();
    }

    @Test
    public void testLoadFromDBObjectExt() throws Exception {
        List<String> strings = new ArrayList<String>();
        strings.add("string1");
        strings.add("string2");
        List<DBObjectExt> objects = new ArrayList<DBObjectExt>();
        objects.add(DBObjectBuilder.start()
                .add(TestObj.InnerObject.Field.NAME, "inner2")
                .add(TestObj.InnerObject.Field.VALUE, 2)
                .get());
        objects.add(DBObjectBuilder.start()
                .add(TestObj.InnerObject.Field.NAME, "inner3")
                .add(TestObj.InnerObject.Field.VALUE, 3)
                .get());
        Date now = new Date();
        DBObjectExt src = DBObjectBuilder.start()
                .add(TestObj.Field.ENUM_VALUE, TestEnum.VALUE)
                .add(TestObj.Field.STRING_VALUE, "testValue")
                .add(TestObj.Field.INT_VALUE, 5)
                .add(TestObj.Field.DATE_VALUE, now)
                .add(TestObj.Field.STRING_LIST_VALUE, strings)
                .add(TestObj.Field.INNER_OBJECT_VALUE, DBObjectBuilder.start()
                        .add(TestObj.InnerObject.Field.NAME, "inner1")
                        .add(TestObj.InnerObject.Field.VALUE, 1))
                .add(TestObj.Field.INNER_OBJECT_LIST_VALUE, objects)
                .get();
        TestObj test = ManagedRepository.loadFromDBObjectExt(TestObj.class, src);

        TestObj.InnerObject i1 = new TestObj.InnerObject();
        i1.name = "inner1";
        i1.value = 1;

        TestObj.InnerObject i2 = new TestObj.InnerObject();
        i2.name = "inner2";
        i2.value = 2;

        TestObj.InnerObject i3 = new TestObj.InnerObject();
        i3.name = "inner3";
        i3.value = 3;

        Assert.assertEquals(TestEnum.VALUE, test.enumValue);
        Assert.assertEquals("testValue", test.stringValue);
        Assert.assertEquals(5, test.intValue);
        Assert.assertEquals(now, test.dateValue);
        AssertExt.assertEquals(strings.toArray(), test.stringListValue);
        Assert.assertEquals(i1, test.innerObjectValue);
        AssertExt.assertEquals(new TestObj.InnerObject[]{i2, i3}, test.innerObjectListValue);
    }

    @Test
    public void testSaveToDBObjectExt() throws Exception {

        boolean asserted = false;
        try {
            ManagedRepository.saveToDBObjectExt(new TestNotAnnotatedEnum());
        } catch (AssertionError e) {
            asserted = true;
        }
        Assert.assertTrue(asserted);

        asserted = false;
        try {
            ManagedRepository.saveToDBObjectExt(new TestNotAnnotatedList());
        } catch (AssertionError e) {
            asserted = true;
        }
        Assert.assertTrue(asserted);

        asserted = false;
        try {
            ManagedRepository.saveToDBObjectExt(new TestNotAnnotatedRef());
        } catch (AssertionError e) {
            asserted = true;
        }
        Assert.assertTrue(asserted);

        TestObj src = new TestObj();
        src.enumValue = TestEnum.TEST;
        src.stringValue = "test";
        src.intValue = 5;
        src.dateValue = new Date();
        src.stringListValue = new ArrayList<String>();
        src.stringListValue.add("test1");
        src.stringListValue.add("test2");
        src.innerObjectValue = new TestObj.InnerObject();
        src.innerObjectValue.name = "inner1";
        src.innerObjectValue.value = 1;
        src.innerObjectListValue = new ArrayList<TestObj.InnerObject>();
        TestObj.InnerObject i2 = new TestObj.InnerObject();
        i2.name = "inner2";
        i2.value = 2;
        src.innerObjectListValue.add(i2);

        TestObj.InnerObject i3 = new TestObj.InnerObject();
        i3.name = "inner3";
        i3.value = 3;
        src.innerObjectListValue.add(i3);

        DBObjectExt test = ManagedRepository.saveToDBObjectExt(src);

        Assert.assertEquals(src.enumValue, test.getV(TestObj.Field.ENUM_VALUE).asEnum(TestEnum.class));
        Assert.assertEquals(src.stringValue, test.getV(TestObj.Field.STRING_VALUE).asString());
        Assert.assertEquals(src.intValue, test.getV(TestObj.Field.INT_VALUE).asInt());
        Assert.assertEquals(src.dateValue, test.getV(TestObj.Field.DATE_VALUE).asDate());
        AssertExt.assertEquals(src.stringListValue.toArray(), test.getV(TestObj.Field.STRING_LIST_VALUE).asList(String.class));
        DBObjectExt inner = test.getV(TestObj.Field.INNER_OBJECT_VALUE).asObject();
        Assert.assertEquals(src.innerObjectValue.name, inner.getV(TestObj.InnerObject.Field.NAME).asString());
        Assert.assertEquals(src.innerObjectValue.value, inner.getV(TestObj.InnerObject.Field.VALUE).asInt());

        List<DBObjectExt> innerList = test.getV(TestObj.Field.INNER_OBJECT_LIST_VALUE).asList(DBObjectExt.class);
        Assert.assertEquals(src.innerObjectListValue.size(), innerList.size());
        for (int i = 0; i < innerList.size(); i++) {
            Assert.assertEquals(src.innerObjectListValue.get(i).name, innerList.get(i).getV(TestObj.InnerObject.Field.NAME).asString());
            Assert.assertEquals(src.innerObjectListValue.get(i).value, innerList.get(i).getV(TestObj.InnerObject.Field.VALUE).asInt());
        }
    }
}
