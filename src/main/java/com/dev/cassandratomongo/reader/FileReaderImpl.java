package com.dev.cassandratomongo.reader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderImpl implements Reader {

    private static final String FILENAME = "filename";
    Properties props;
    private static Logger logger = LoggerFactory
        .getLogger(FileReaderImpl.class);

    public FileReaderImpl(Properties props) {
        this.props = props;
    }

    public List<String> read() throws Exception {
        List<String> lines = new ArrayList<String>();
        String fileName = props.getProperty(FILENAME);

        InputStream stream = FileReaderImpl.class.getClassLoader()
            .getResourceAsStream(fileName);
        System.out.println(stream != null);

        BufferedReader in = new BufferedReader(new InputStreamReader(stream));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
            lines.add(line);
        }

        return lines;
    }

}
