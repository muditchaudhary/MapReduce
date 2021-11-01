package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Reduce writer class for reduce function. Each reducer has only 1 writer based on the assigned partition
 */
public class ReduceResultWriter {


    private Integer numWorkers;
    private String reducerPartition;
    private String outputFileLocation;
    private FileWriter reduceWriter;


    /**
     * Constructor method
     * @param numWorkers
     * @param outputFileLocation
     * @param reducerPartition
     * @throws IOException
     */
    public ReduceResultWriter(Integer numWorkers, String outputFileLocation, String reducerPartition) throws IOException {
        this.numWorkers = numWorkers;
        this.outputFileLocation = outputFileLocation;
        this.reducerPartition = reducerPartition;

        for (Integer i = 0; i< this.numWorkers; i++){
            Path outputFileFullName = Paths.get(this.outputFileLocation, "result_part_"+this.reducerPartition+".txt");
            reduceWriter = new FileWriter(outputFileFullName.toString());
        }

    }

    /**
     * Write result to file from the user using key-value pairs
     * @param key
     * @param value
     * @throws IOException
     */
    public void writeResult(String key, Object value) throws IOException {
        String result = key + " " + value + "\n";
        reduceWriter.write(result);
    }

    /**
     * Close writer
     * @throws IOException
     */
    public void reduceWriterClose() throws IOException {
        reduceWriter.close();
    }
}
