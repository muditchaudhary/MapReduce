package com.mapreduce;

import java.io.FileWriter;
import java.io.IOException;

public interface Mapper {
    void Map(String line, FileWriter result ) throws IOException;

    static String MapResult(String key, Object value){
        String result = key + " "+ value + "\n";
        return result;
    }
}
