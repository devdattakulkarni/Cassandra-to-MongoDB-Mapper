package com.dev.cassandratomongo.writer;

import java.util.List;

public interface Writer {

    public void write(List<String> data) throws Exception;

}
