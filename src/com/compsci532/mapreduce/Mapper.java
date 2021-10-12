package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;

public interface Mapper  {
    void map(String key, String value, FileWriter result ) throws IOException;

    static String mapResult(String key, Object value){
        String result = key + " "+ value + "\n";
        return result;
    }
}
