package com.hitsoft.mongo.repository;

import com.hitsoft.mongo.basic.BasicRepository;
import com.hitsoft.mongo.basic.Connection;

/**
 * Created by IntelliJ IDEA.
 * User: smeagol
 * Date: 07.11.11
 * Time: 16:48
 */
public class Basic {

    public static BasicRepository getRepository(String collectionName) {
        return BasicRepository.getRepository(Connection.getInstance(), collectionName);
    }

    public static BasicRepository getTestRepository() {
        return getRepository("test");
    }
}
