package com.hitsoft.mongo.basic;

import com.hitsoft.mongo.repository.Basic;
import com.hitsoft.test.AssertExt;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * User: smeagol
 * Date: 05.11.11
 * Time: 14:37
 */
public class BasicRepositoryTest {
    @Before
    public void setUp() throws Exception {
        Connection.setupTestInstance();
        Connection.getInstance().dropDatabase();
    }

    @After
    public void tearDown() throws Exception {
        Connection.getInstance().dropDatabase();
    }

    private static enum Field {
        NAME,
        VALUE
    }

    @Test
    public void testSave() throws Exception, BasicRepository.NotUniqueCondition {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj = rep.save(DBObjectBuilder.single(Field.NAME, "smeagol"));
        DBObjectExt obj1 = rep.findById(obj.getId());
        Assert.assertEquals(obj, obj1);
    }

    @Test
    public void testRemove() throws Exception, BasicRepository.NotUniqueCondition {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj = rep.save(DBObjectBuilder.single(Field.NAME, "smeagol"));
        DBObjectExt obj1 = rep.findById(obj.getId());
        Assert.assertEquals(obj, obj1);
        rep.remove(obj1);
        obj1 = rep.findById(obj.getId());
        Assert.assertNull(obj1);
    }

    @Test
    public void testFindAll() throws Exception {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj = rep.save(DBObjectBuilder.single(Field.NAME, "smeagol"));
        List<DBObjectExt> test = rep.find().exec();
        Assert.assertEquals(1, test.size());
        Assert.assertEquals(obj, test.get(0));
        DBObjectExt obj1 = rep.save(DBObjectBuilder.single(Field.NAME, "gollum"));
        test = rep.find().exec();
        Assert.assertEquals(2, test.size());
        Assert.assertEquals(obj, test.get(0));
        Assert.assertEquals(obj1, test.get(1));
    }

    @Test
    public void testIterate() throws Exception {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj = rep.save(DBObjectBuilder.single(Field.NAME, "smeagol"));
        DBObjectExt obj1 = rep.save(DBObjectBuilder.single(Field.NAME, "gollum"));
        final int[] processed = {0};
        rep.find()
                .where(SearchBuilder.start().equal(Field.NAME, "smeagol"))
                .orderBy(SortBuilder.start().asc(Field.NAME))
                .execIterate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        processed[0]++;
                    }
                });
        Assert.assertEquals(1, processed[0]);
        processed[0] = 0;
        rep.find()
                .where(SearchBuilder.start().equal(Field.NAME, "smeagol"))
                .orderBy(SortBuilder.start().asc(Field.NAME))
                .execIterateUpdate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        obj.put(Field.NAME, "gollum");
                        processed[0]++;
                    }
                });
        Assert.assertEquals(1, processed[0]);
        processed[0] = 0;
        rep.find()
                .where(SearchBuilder.start().equal(Field.NAME, "smeagol"))
                .orderBy(SortBuilder.start().asc(Field.NAME))
                .execIterate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        processed[0]++;
                    }
                });
        Assert.assertEquals(0, processed[0]);

        DBObjectExt o5 = rep.save(DBObjectBuilder.single(Field.VALUE, 5));
        DBObjectExt o3 = rep.save(DBObjectBuilder.single(Field.VALUE, 3));
        DBObjectExt o2 = rep.save(DBObjectBuilder.single(Field.VALUE, 2));
        DBObjectExt o4 = rep.save(DBObjectBuilder.single(Field.VALUE, 4));
        DBObjectExt o1 = rep.save(DBObjectBuilder.single(Field.VALUE, 1));
        processed[0] = 1;
        rep.find()
                .where(SearchBuilder.start().gt(Field.VALUE, 0))
                .orderBy(SortBuilder.start().asc(Field.VALUE))
                .execIterate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        Assert.assertEquals(processed[0], obj.getV(Field.VALUE).asInt());
                        processed[0]++;
                    }
                });
        Assert.assertEquals(6, processed[0]);
        processed[0] = 5;
        rep.find()
                .where(SearchBuilder.start().gt(Field.VALUE, 0))
                .orderBy(SortBuilder.start().desc(Field.VALUE))
                .execIterate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        Assert.assertEquals(processed[0], obj.getV(Field.VALUE).asInt());
                        processed[0]--;
                    }
                });
        Assert.assertEquals(0, processed[0]);
    }

    @Test
    public void testFindOne() throws Exception, BasicRepository.NotUniqueCondition {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj = rep.save(DBObjectBuilder.single(Field.NAME, "smeagol"));
        DBObjectExt obj1 = rep.save(DBObjectBuilder.single(Field.NAME, "gollum"));
        DBObjectExt obj2 = rep.save(DBObjectBuilder.single(Field.NAME, "gollum"));
        DBObjectExt test = rep.find().where(SearchBuilder.start().equal(Field.NAME, "smeagol")).execOne();
        Assert.assertEquals(obj, test);
        test = rep.find().where(SearchBuilder.start().equal(Field.NAME, "some")).execOne();
        Assert.assertNull(test);
        boolean error = false;
        try {
            rep.find().where(SearchBuilder.start().equal(Field.NAME, "gollum")).execOne();
        } catch (BasicRepository.NotUniqueCondition e) {
            error = true;
        }
        Assert.assertTrue(error);
    }

    @Test
    public void testFindById() throws Exception, BasicRepository.NotUniqueCondition {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj = rep.save(DBObjectBuilder.single(Field.NAME, "smeagol"));
        DBObjectExt test = rep.findById(obj.getId());
        Assert.assertEquals(obj, test);
    }

    @Test
    public void testFindDoExec() throws Exception {
        BasicRepository rep = Basic.getTestRepository();
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "smeagol")
                .add(Field.VALUE, 1)
                .get());
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        List<DBObjectExt> test = rep.find().exec();
        Assert.assertEquals(2, test.size());
        for (DBObjectExt t : test) {
            Assert.assertNotNull(t.getId());
            Assert.assertFalse(t.getV(Field.NAME).isNull());
            Assert.assertFalse(t.getV(Field.VALUE).isNull());
        }

        test = rep.find()
                .idsOnly()
                .exec();
        Assert.assertEquals(2, test.size());
        for (DBObjectExt t : test) {
            Assert.assertNotNull(t.getId());
            Assert.assertTrue(t.getV(Field.NAME).isNull());
            Assert.assertTrue(t.getV(Field.VALUE).isNull());
        }

        test = rep.find()
                .fieldsShow(new Enum[]{Field.NAME})
                .exec();
        Assert.assertEquals(2, test.size());
        for (DBObjectExt t : test) {
            Assert.assertNotNull(t.getId());
            Assert.assertFalse(t.getV(Field.NAME).isNull());
            Assert.assertTrue(t.getV(Field.VALUE).isNull());
        }

        test = rep.find()
                .fieldsHide(new Enum[]{Field.VALUE})
                .exec();
        Assert.assertEquals(2, test.size());
        for (DBObjectExt t : test) {
            Assert.assertNotNull(t.getId());
            Assert.assertFalse(t.getV(Field.NAME).isNull());
            Assert.assertTrue(t.getV(Field.VALUE).isNull());
        }

        test = rep.find()
                .orderBy(new Enum[]{Field.VALUE})
                .exec();
        Assert.assertEquals(2, test.size());
        int val = 1;
        for (DBObjectExt t : test) {
            Assert.assertEquals(val, t.getV(Field.VALUE).asInt());
            val++;
        }

        test = rep.find()
                .orderBy(SortBuilder.start().desc(Field.VALUE))
                .exec();
        Assert.assertEquals(2, test.size());
        val = 2;
        for (DBObjectExt t : test) {
            Assert.assertEquals(val, t.getV(Field.VALUE).asInt());
            val--;
        }

        test = rep.find()
                .orderBy(new Enum[]{Field.VALUE})
                .limit(1)
                .exec();
        Assert.assertEquals(1, test.size());
        for (DBObjectExt t : test) {
            Assert.assertEquals(1, t.getV(Field.VALUE).asInt());
        }

        test = rep.find()
                .orderBy(new Enum[]{Field.VALUE})
                .limit(1)
                .skip(1)
                .exec();
        Assert.assertEquals(1, test.size());
        for (DBObjectExt t : test) {
            Assert.assertEquals(2, t.getV(Field.VALUE).asInt());
        }
    }

    @Test
    public void testFindDoExecConditions() throws Exception {
        // equal(Enum field, Object value) {
        // gt(Enum field, Object value) {
        // lt(Enum field, Object value) {
        // gte(Enum field, Object value) {
        // lte(Enum field, Object value) {
        // between(Enum field, Object from, Object to) {
        // between(Enum field, Object from, Object to, boolean includeFrom, boolean includeTo) {
        // all(Enum field, SearchBuilder condition) {
        // all(Enum field, Collection <SearchBuilder> conditions) {
        // exists(Enum field, boolean value) {
        // mod(Enum field, int mod, int value) {
        // ne(Enum field, Object value) {
        // in(Enum field, List values) {
        // nin(Enum field, List values) {
        // nor(Enum field, Collection<SearchBuilder> conditions) {
        // or(Enum field, Collection<SearchBuilder> conditions) {
        // and(Enum field, Collection<SearchBuilder> conditions) {
        // size(Enum field, int size) {
        // type(Enum field, FieldType type) {
        // regex(Enum field, String expr) {
        // regex(Enum field, String expr, int flags) {
        // TODO implement BasicRepositoryTest . testFindDoExecConditions
    }

    @Test
    public void testFindExec() throws Exception, BasicRepository.NotUniqueCondition {
        BasicRepository rep = Basic.getTestRepository();
        DBObjectExt obj1 = rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "smeagol")
                .add(Field.VALUE, 1)
                .get());
        DBObjectExt obj2 = rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        DBObjectExt obj3 = rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 3)
                .get());

        SearchBuilder conditions = SearchBuilder.start()
                .equal(Field.NAME, "gollum");
        SortBuilder sort = SortBuilder.start().asc(Field.VALUE);

        List<DBObjectExt> test = rep.find()
                .where(conditions)
                .orderBy(sort)
                .exec();
        AssertExt.assertEquals(new DBObjectExt[]{obj2, obj3}, test);

        Assert.assertEquals(obj2, rep.find()
                .where(conditions)
                .orderBy(sort)
                .execFirst());

        Assert.assertEquals(obj1, rep.find()
                .where(SearchBuilder.start().equal(Field.NAME, "smeagol"))
                .execOne());

        boolean error = false;
        try {
            rep.find()
                    .where(SearchBuilder.start().equal(Field.NAME, "gollum"))
                    .execOne();
        } catch (BasicRepository.NotUniqueCondition e) {
            error = true;
        }
        Assert.assertTrue(error);

        Assert.assertNull(rep.find()
                .where(SearchBuilder.start().equal(Field.NAME, "smeagol1"))
                .execOne());

        test = rep.find().execFilter(new BasicRepository.Filter() {
            @Override
            public boolean accept(DBObjectExt obj) {
                return obj.getV(Field.VALUE).asInt() == 3;
            }
        });
        AssertExt.assertEquals(new DBObjectExt[]{obj3}, test);

        final int[] count = {0};
        rep.find()
                .where(conditions)
                .orderBy(sort)
                .execIterate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        Assert.assertEquals("gollum", obj.getV(Field.NAME).asString());
                        count[0]++;
                    }
                });
        Assert.assertEquals(count[0], 2);

        count[0] = 0;
        rep.find()
                .where(conditions)
                .orderBy(sort)
                .execIterateUpdate(new BasicRepository.Processor() {
                    @Override
                    public void process(DBObjectExt obj) {
                        Assert.assertEquals("gollum", obj.getV(Field.NAME).asString());
                        obj.put(Field.NAME, "smeagol");
                        count[0]++;
                    }
                });
        Assert.assertEquals(count[0], 2);

        count[0] = 0;
        for (DBObjectExt obj : rep.find().exec()) {
            Assert.assertEquals("smeagol", obj.getV(Field.NAME).asString());
            count[0]++;
        }
        Assert.assertEquals(count[0], 3);
    }

    @Test
    public void testCount() throws Exception {
        BasicRepository rep = Basic.getTestRepository();
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "smeagol")
                .add(Field.VALUE, 1)
                .get());
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        Assert.assertEquals(2, rep.count(SearchBuilder.start()));
        Assert.assertEquals(1, rep.count(SearchBuilder.start().equal(Field.VALUE, 2)));
    }

    @Test
    public void testUpdate() throws Exception {
        // inc(Enum field, Number value) {
        // set(Enum field, Object value) {
        // unset(Enum field) {
        // push(Enum field, Object value) {
        // addToSet(Enum field, Object value) {
        // popLast(Enum field) {
        // popFirst(Enum field) {
        // pull(Enum field, Object value) {
        // rename(Enum oldField, Enum newField) {
        // TODO implement BasicRepositoryTest . testUpdate
    }

    @Test
    public void testDistinct() throws Exception {
        BasicRepository rep = Basic.getTestRepository();
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "smeagol")
                .add(Field.VALUE, 1)
                .get());
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        AssertExt.assertEquals(new String[]{"smeagol", "gollum"}, rep.distinct(Field.NAME, SearchBuilder.start()));
        AssertExt.assertEquals(new Integer[]{1, 2}, rep.distinct(Field.VALUE, SearchBuilder.start()));
    }

    @Test
    public void testDistinctStrings() throws Exception {
        BasicRepository rep = Basic.getTestRepository();
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "smeagol")
                .add(Field.VALUE, 1)
                .get());
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        rep.save(DBObjectBuilder.start()
                .add(Field.NAME, "gollum")
                .add(Field.VALUE, 2)
                .get());
        AssertExt.assertEquals(new String[]{"smeagol", "gollum"}, rep.distinctStrings(Field.NAME, SearchBuilder.start()));
        AssertExt.assertEquals(new String[]{"1", "2"}, rep.distinctStrings(Field.VALUE, SearchBuilder.start()));
    }
}
