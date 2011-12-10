package com.dev.cassandratomongo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.cassandra.service.StringKeyIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/*
 * Basic working client
 */
public class HectorClient {

    public static void main(String[] args) {
        Cluster myCluster = null;
        List<String> columnNamesList = new ArrayList<String>();
        try {

            myCluster = HFactory.getOrCreateCluster("Test Cluster",
                "localhost:9160");

            String devdattaKeySpace = "DevdattaKeyspace";
            String columnFamily = "DevdattaStandard";

            ColumnFamilyDefinition cfDef = HFactory
                .createColumnFamilyDefinition(devdattaKeySpace, columnFamily,
                    ComparatorType.BYTESTYPE);

            int replicationFactor = 1;
            KeyspaceDefinition newKeyspaceDef = HFactory
                .createKeyspaceDefinition(devdattaKeySpace,
                    ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor, Arrays
                        .asList(cfDef));

            Keyspace ksp = HFactory.createKeyspace(devdattaKeySpace, myCluster);

            System.out.println("-- Inserting data into Cassandra --");
            insertData(ksp, columnFamily, columnNamesList);
            System.out.println("-- Reading data back from Cassandra --");
            getData(ksp, columnFamily, columnNamesList);

        } catch (Exception exp) {
            exp.printStackTrace();
            HFactory.shutdownCluster(myCluster);
        }
    }

    private static void insertData(Keyspace ksp, String columnFamily,
        List<String> columnNamesList) throws IOException {
        Mutator<String> mutator = HFactory.createMutator(ksp, StringSerializer
            .get());

        BufferedReader in = new BufferedReader(new FileReader(
            "/etc/CassandraInput/input.txt"));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println("--" + line + "--");
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

    private static void getData(Keyspace ksp, String columnFamily,
        List<String> columnList) {

        StringKeyIterator keyIterator = new StringKeyIterator(ksp, columnFamily);

        Iterator<String> keys = keyIterator.iterator();

        //System.out.println("Column Names:" + columnList.toString());
        while (keys.hasNext()) {
            String key = keys.next();
            System.out.println("Key:" + key);

            ColumnQuery<String, String, String> columnQuery = HFactory
                .createStringColumnQuery(ksp);

            for (int i = 0; i < columnList.size(); i++) {
                columnQuery.setColumnFamily(columnFamily).setKey(key).setName(
                    columnList.get(i));

                QueryResult<HColumn<String, String>> result = columnQuery
                    .execute();

                HColumn<String, String> cols = result.get();
                if (cols != null) {
                    System.out.println(cols.getName() + ":" + cols.getValue());
                }
            }

        }
    }

    private void basicWorkingQuery(Keyspace ksp, String columnFamily) {
        ColumnQuery<String, String, String> columnQuery = HFactory
            .createStringColumnQuery(ksp);
        columnQuery.setColumnFamily(columnFamily).setKey("sachintendulkar")
            .setName("first");

        QueryResult<HColumn<String, String>> result = columnQuery.execute();

        HColumn<String, String> cols = result.get();
        System.out.println("Column name:" + cols.getName() + " Column value:"
            + cols.getValue());
    }
}
