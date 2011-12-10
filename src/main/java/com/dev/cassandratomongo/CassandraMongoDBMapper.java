package com.dev.cassandratomongo;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.StringKeyIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class CassandraMongoDBMapper {

    public static void main(String[] args) {
        Cluster myCluster = null;
        Keyspace ksp = null;

        List<String> columnNamesList = new ArrayList<String>();
        List<String> lines = new ArrayList<String>();

        String devdattaKeySpace = "DevdattaKeyspace";
        String columnFamily = "DevdattaStandard";
        String cassandraClusterName = "Test Cluster";
        String cassandraHostAndPort = "localhost:9160";
        String mongoDBHost = "localhost";
        int mongoDBPort = 27017;
        int replicationFactor = 1;

        Mongo m = null;
        DB db = null;

        try {

            // initializeCassandra(myCluster, cassandraClusterName,
            // cassandraHostAndPort, devdattaKeySpace, columnFamily,
            // replicationFactor, ksp);

            myCluster = HFactory.getOrCreateCluster(cassandraClusterName,
                cassandraHostAndPort);
            ColumnFamilyDefinition cfDef = HFactory
                .createColumnFamilyDefinition(devdattaKeySpace, columnFamily,
                    ComparatorType.BYTESTYPE);

            KeyspaceDefinition newKeyspaceDef = HFactory
                .createKeyspaceDefinition(devdattaKeySpace,
                    ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays
                        .asList(cfDef));

            myCluster.dropKeyspace(devdattaKeySpace);
            ksp = HFactory.createKeyspace(devdattaKeySpace, myCluster);
            myCluster.addKeyspace(newKeyspaceDef);

            System.out.println("-- Reading Data -- ");
            readData(lines);

            // Insert and query back data from Cassandra
            System.out.println("\n-- Inserting data into Cassandra --");
            insertDataIntoCassandra(lines, ksp, columnFamily, columnNamesList);
            System.out.println("\n-- Reading data back from Cassandra --");
            getDataFromCassandra(ksp, columnFamily, columnNamesList);

            m = new Mongo(mongoDBHost, mongoDBPort);
            db = m.getDB(devdattaKeySpace);
            db.dropDatabase();

            // Insert and query back data from MongoDB
            // insertDataIntoMongoDB(db, columnFamily);

            System.out.println("\n-- Inserting data into MongoDB --");
            getDataFromCassandraAndInsertIntoMongo(ksp, columnFamily,
                columnNamesList, db);

            System.out.println("\n-- Reading data back from MongoDB --");
            getDataFromMongoDB(db, columnFamily);

        } catch (Exception exp) {
            exp.printStackTrace();
            HFactory.shutdownCluster(myCluster);
        }
    }

    private static void initializeCassandra(Cluster myCluster,
        String cassandraClusterName, String cassandraHostAndPort,
        String keyspace, String columnFamily, int replicationFactor,
        Keyspace ksp) {
        myCluster = HFactory.getOrCreateCluster(cassandraClusterName,
            cassandraHostAndPort);
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
            keyspace, columnFamily, ComparatorType.BYTESTYPE);

        KeyspaceDefinition newKeyspaceDef = HFactory.createKeyspaceDefinition(
            keyspace, ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays
                .asList(cfDef));

        myCluster.dropKeyspace(keyspace);
        ksp = HFactory.createKeyspace(keyspace, myCluster);
        myCluster.addKeyspace(newKeyspaceDef);
    }

    private static void insertDataIntoMongoDB(DB db, String collectionName) {
        DBCollection coll = db.getCollection(collectionName);

        BasicDBObject doc = new BasicDBObject();
        doc.put("key", "sachintendulkar");
        doc.put("first", "Sachin");

        coll.insert(doc);
    }

    private static void getDataFromMongoDB(DB db, String collectionName) {
        DBCollection coll = db.getCollection(collectionName);
        DBCursor cursor = coll.find();

        Iterator<DBObject> docIter = cursor.iterator();
        while (docIter.hasNext()) {
            DBObject myDoc = docIter.next();
            System.out.println(myDoc);
        }
    }

    private static void readData(List<String> lines) throws Exception {

        BufferedReader in = new BufferedReader(new FileReader(
            "/etc/CassandraInput/input.txt"));

        // InputStream is = new ByteArrayInputStream(inputData.getBytes());
        // InputStreamReader reader = new InputStreamReader(is, ENCODING);
        // BufferedReader in = new BufferedReader(new FileReader(fileHandle));

        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
            lines.add(line);
        }
    }

    private static void insertDataIntoCassandra(List<String> lines,
        Keyspace ksp, String columnFamily, List<String> columnNamesList)
        throws IOException {
        Mutator<String> mutator = HFactory.createMutator(ksp, StringSerializer
            .get());

        // BufferedReader in = new BufferedReader(new FileReader(
        // "/etc/CassandraInput/input.txt"));
        // String line = null;
        // while ((line = in.readLine()) != null) {
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            StringTokenizer tokenizer = new StringTokenizer(line, ",");
            boolean firstPair = true;
            String key = null;
            String columnName = null;
            String columnValue = null;
            while (tokenizer.hasMoreTokens()) {
                String columnValuePair = tokenizer.nextToken();
                String[] columnAndValue = columnValuePair.split(":");
                if (firstPair) {
                    firstPair = false;
                    key = columnAndValue[1];
                    columnName = key;
                    columnNamesList.add(columnName);
                } else {
                    columnName = columnAndValue[0];
                    if (!columnNamesList.contains(columnName)) {
                        columnNamesList.add(columnName);
                    }
                    columnValue = columnAndValue[1];
                    // System.out.println("Key:" + key + " Column Name:"
                    // + columnName + " Column Value:" + columnValue);

                    mutator.insert(key, columnFamily, HFactory
                        .createStringColumn(columnName, columnValue));
                }
            }
        }
    }

    private static void getDataFromCassandraAndInsertIntoMongo(Keyspace ksp,
        String columnFamily, List<String> columnList, DB db) {

        DBCollection coll = db.getCollection(columnFamily);

        StringKeyIterator keyIterator = new StringKeyIterator(ksp, columnFamily);

        Iterator<String> keys = keyIterator.iterator();

        // System.out.println("Column Names:" + columnList.toString());
        while (keys.hasNext()) {
            String key = keys.next();
            // System.out.println("Key:" + key);

            ColumnQuery<String, String, String> columnQuery = HFactory
                .createStringColumnQuery(ksp);

            BasicDBObject doc = new BasicDBObject();
            for (int i = 0; i < columnList.size(); i++) {
                columnQuery.setColumnFamily(columnFamily).setKey(key).setName(
                    columnList.get(i));

                QueryResult<HColumn<String, String>> result = columnQuery
                    .execute();

                HColumn<String, String> cols = result.get();
                if (cols != null) {
                    //System.out.println(cols.getName() + ":" + cols.getValue());
                    doc.put(cols.getName(), cols.getValue());
                }
            }
            coll.insert(doc);
        }
    }

    private static void getDataFromCassandra(Keyspace ksp, String columnFamily,
        List<String> columnList) {

        StringKeyIterator keyIterator = new StringKeyIterator(ksp, columnFamily);

        Iterator<String> keys = keyIterator.iterator();

        // System.out.println("Column Names:" + columnList.toString());
        while (keys.hasNext()) {
            String key = keys.next();
            // System.out.println("Key:" + key);

            ColumnQuery<String, String, String> columnQuery = HFactory
                .createStringColumnQuery(ksp);

            for (int i = 0; i < columnList.size(); i++) {
                columnQuery.setColumnFamily(columnFamily).setKey(key).setName(
                    columnList.get(i));

                QueryResult<HColumn<String, String>> result = columnQuery
                    .execute();

                HColumn<String, String> cols = result.get();
                if (cols != null) {
                    System.out.print(cols.getName() + ":" + cols.getValue());
                }
            }
            System.out.println();
        }
    }

}
