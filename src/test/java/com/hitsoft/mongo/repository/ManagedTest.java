package com.hitsoft.mongo.repository;

import com.hitsoft.mongo.basic.Connection;
import com.hitsoft.mongo.basic.SearchBuilder;
import com.hitsoft.mongo.managed.ListField;
import com.hitsoft.mongo.managed.ManagedObject;
import com.hitsoft.mongo.managed.MongoRef;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 07.11.11
 * Time: 17:59
 */
public class ManagedTest {

    @Before
    public void setUp() throws Exception {
        Connection.setupTestInstance();
        Connection.getInstance().dropDatabase();
    }

    @After
    public void tearDown() throws Exception {
        Connection.getInstance().dropDatabase();
    }

    public static class Company extends ManagedObject {

        public static enum Field {
            NAME,
            YEAR,
            OFFICES
        }

        public String name;
        public int age;
        @ListField(type = ObjectId.class)
        @MongoRef(ref = Address.class)
        public List<ObjectId> offices = new ArrayList<ObjectId>();

        public static class Search {
            public static SearchBuilder byName(String name) {
                return SearchBuilder.start()
                        .regex(Field.NAME, name, Pattern.CASE_INSENSITIVE);
            }
        }

        public static Company apply(String name, int age) {
            Company result = new Company();
            result.name = name;
            result.age = age;
            return result;
        }
    }

    public static class User extends ManagedObject {
        public String firstName;
        public String lastName;
        public Date birthDay;
        @MongoRef(ref = Address.class)
        public ObjectId homeAddress;
        @MongoRef(ref = Company.class)
        public ObjectId company;

        public static User apply(String firstName, String lastName, Company company) {
            User result = new User();
            result.firstName = firstName;
            result.lastName = lastName;
            result.company = company.id();
            return result;
        }
    }

    public static class Address extends ManagedObject {
        public String country;
        public String city;
        public String street;
        public String address;

        public static Address apply(String country, String city, String street, String address) {
            Address result = new Address();
            result.country = country;
            result.city = city;
            result.street = street;
            result.address = address;
            return result;
        }
    }


    @Test
    public void testFind() throws Exception {
        Address hitOffice = Managed.save(Address.apply("Russia", "Novosibirsk", "Inzhenernaya", "4a - 816"));
        Company company = Company.apply("Hitsoft", 10);
        company.offices.add(hitOffice.id());
        company = Managed.save(company);

        User user = Managed.save(User.apply("Konstantin", "Borisov", company));

        List<User> test = Managed.find(User.class).exec();

        Assert.assertEquals(1, test.size());
        Assert.assertEquals(user, test.get(0));
    }

    @Test
    public void testFindById() throws Exception {

    }

    @Test
    public void testCount() throws Exception {

    }

    @Test
    public void testEnsureIndex() throws Exception {

    }

    @Test
    public void testUpdate() throws Exception {

    }

    @Test
    public void testDistinct() throws Exception {

    }

    @Test
    public void testDistinctStrings() throws Exception {

    }
}
