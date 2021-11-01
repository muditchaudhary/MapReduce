package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Interface for implementing the user-defined reducer class
 */
public interface Reducer{

    /**
     * Abstract function definition for reducer function to be implemented by the user
     *
     * @param key
     * @param values
     * @param writer
     * @throws IOException
     */
    void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException;

}
