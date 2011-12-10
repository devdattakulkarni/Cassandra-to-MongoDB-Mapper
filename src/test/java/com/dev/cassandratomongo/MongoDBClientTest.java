package com.dev.cassandratomongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class MongoDBClientTest {

    private Properties props;
    private MongoDBClient mongo;

    @Before
    public void setup() throws Exception {
        props = new Properties();
    }

    @Test
    public void testWriteToAndReadFromMongoDB() throws Exception {
        props.setProperty("host", "localhost");
        props.setProperty("port", "27017");
        props.setProperty("db", "mydb");
        props.setProperty("collection", "collection7");

        mongo = new MongoDBClient(props);

        List<String> input = new ArrayList<String>();
        input.add("key:HackerNews, office:MountainView, employeeCount:10");
        mongo.write(input);
        List<String> output = mongo.read();
        System.out.println("--- Data Read Back ---");
        System.out.println(output);

    }

}
