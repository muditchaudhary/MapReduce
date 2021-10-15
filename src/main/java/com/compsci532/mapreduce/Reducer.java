package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public interface Reducer{
    void reduce(String key, ArrayList<String> values, FileWriter result) throws IOException;

    static String reduceResult(String key, Object value){
        String result = key + " "+ value + "\n";
        return result;
    }

}
