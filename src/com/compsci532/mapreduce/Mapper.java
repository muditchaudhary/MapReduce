package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;

public abstract class Mapper extends Worker {
    abstract void map(String key, String value, FileWriter result ) throws IOException;

    public Mapper(){
        super("Mapper");
    }

    static String mapResult(String key, Object value){
        String result = key + " "+ value + "\n";
        return result;
    }
}
