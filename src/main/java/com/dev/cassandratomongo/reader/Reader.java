package com.dev.cassandratomongo.reader;

import java.util.List;

public interface Reader {

    public List<String> read() throws Exception;
    
}
