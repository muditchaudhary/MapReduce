package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Interface for implementing the user-defined mapper class
 */
public interface Mapper  {
    /**
     *
     * Abstract function definition for Mapper function to be implemented by the user
     *
     * @param key
     * @param value
     * @param writer
     * @throws IOException
     */
     void map(String key, String value, MapResultWriter writer) throws IOException;

}
