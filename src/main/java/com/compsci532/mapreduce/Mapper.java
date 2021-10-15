package com.compsci532.mapreduce;

public interface Mapper  {

    static String mapResult(String key, Object value){
        String result = key + " "+ value + "\n";
        return result;
    }
}
