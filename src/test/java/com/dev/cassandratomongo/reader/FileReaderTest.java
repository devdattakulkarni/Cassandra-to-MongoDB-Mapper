package com.dev.cassandratomongo.reader;

import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderTest {

    private static Logger logger = LoggerFactory
        .getLogger(FileReaderTest.class);

    FileReaderImpl fileReader;
    Properties props;

    @Before
    public void setup() throws Exception {

    }

    @Test
    public void testFileFound() throws Exception {
        props = new Properties();
        props.setProperty("filename", "input.txt");
        fileReader = new FileReaderImpl(props);
        List<String> lines = fileReader.read();
        Assert.assertEquals(lines.size() > 0, true);
    }
}
