package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class Reducer extends Worker{
    abstract void reduce(String key, ArrayList<String> values, FileWriter result) throws IOException;

    public Reducer(){
        super("Reducer");
    }

    static String reduceResult(String key, Object value){
        String result = key + " "+ value + "\n";
        return result;
    }

}
