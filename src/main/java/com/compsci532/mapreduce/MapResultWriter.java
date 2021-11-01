package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Result writer class for mapper function
 */
public class MapResultWriter {

    private Integer numWorkers;
    private String intermediateFileLocation;
    private String mapperID;
    private ArrayList<FileWriter> WriterList= new ArrayList<>();

    /**
     * Constructor method
     * @param numWorkers
     * @param intermediateFileLocation
     * @param mapperID
     * @throws IOException
     */
    public MapResultWriter(Integer numWorkers, String intermediateFileLocation, String mapperID) throws IOException {
        this.numWorkers = numWorkers;
        this.intermediateFileLocation = intermediateFileLocation;
        this.mapperID = mapperID;

        // Create num_workers writers for each mapper
        for (Integer i = 0; i< this.numWorkers; i++){
            Path intermediateFileFullName = Paths.get(this.intermediateFileLocation, this.mapperID+"_"+i+".txt");
            WriterList.add(new FileWriter(intermediateFileFullName.toString()));
        }

    }

    /**
     * Method for user to write result into the intermediate files. Puts the key-value pair in appropriate intermediate
     * file partition based on key's hash value and num_workers
     * @param key
     * @param value
     * @throws IOException
     */
    public void writeResult(String key, Object value) throws IOException {
        String result = key + " " + value + "\n";
        Integer key_partition = Math.abs(key.hashCode()%this.numWorkers);
        WriterList.get(key_partition).write(result);
    }

    /**
     * Closes all writers for the mapper
     * @throws IOException
     */
    public void mapWriterClose() throws IOException {
        for (Integer i = 0; i< this.numWorkers; i++){
            WriterList.get(i).close();
        }
    }
}
