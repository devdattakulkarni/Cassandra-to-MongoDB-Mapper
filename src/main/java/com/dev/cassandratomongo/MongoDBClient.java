package com.dev.cassandratomongo;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dev.cassandratomongo.reader.Reader;
import com.dev.cassandratomongo.writer.Writer;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBClient implements Reader, Writer {

    private static final Logger logger = LoggerFactory
        .getLogger(MongoDBClient.class);

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DBNAME = "db";
    private static final String COLLECTION = "collection";

    private Properties props;
    private Mongo mongo;
    private DB db;

    private String collectionName;

    public MongoDBClient(Properties properties) {
        props = properties;
        collectionName = props.getProperty(COLLECTION);
        try {
            mongo = new Mongo(props.getProperty(HOST), Integer.parseInt(props
                .getProperty(PORT)));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            logger.debug(e.getMessage());
        } catch (MongoException e) {
            e.printStackTrace();
            logger.debug(e.getMessage());
        }

        db = mongo.getDB(props.getProperty(DBNAME));
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public List<String> read() throws Exception {
        List<String> data = new ArrayList<String>();

        DBCollection coll = db.getCollection(collectionName);
        DBCursor cursor = coll.find();

        Iterator<DBObject> docIter = cursor.iterator();
        while (docIter.hasNext()) {
            DBObject myDoc = docIter.next();
            System.out.println(myDoc);
            data.add(myDoc.toString());
        }
        return data;
    }

    public void write(List<String> data) throws Exception {
        DBCollection coll = db.getCollection(collectionName);
        for (int i = 0; i < data.size(); i++) {
            BasicDBObject doc = new BasicDBObject();
            String line = data.get(i);

            StringTokenizer tokenizer = new StringTokenizer(line, ",");

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                String[] nameValue = token.split(":");
                if (nameValue != null) {
                    doc.put(nameValue[0], nameValue[1]);
                }
            }
            coll.insert(doc);
        }
    }
}
