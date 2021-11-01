package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public interface Reducer{
    void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException;

}
