package com.dev.cassandratomongo;

import java.net.UnknownHostException;
import java.util.Set;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoException;

/* 
 * Basic working client
 */
public class MongoClient {

    public static void main(String[] args) throws UnknownHostException,
        MongoException {

        Mongo m = new Mongo("localhost", 27017);

        DB db = m.getDB("mydb");

        Set<String> colls = db.getCollectionNames();

        for (String s : colls) {
            System.out.println(s);
        }
        
        DBCollection coll = db.getCollection("collection1");
        
        BasicDBObject doc = new BasicDBObject();
        doc.put("key", "sachintendulkar");
        doc.put("first", "Sachin");
        
        coll.insert(doc);
        
        DBObject myDoc = coll.findOne();
        System.out.println(myDoc);


    }
}
