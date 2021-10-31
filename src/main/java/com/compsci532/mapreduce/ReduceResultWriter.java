package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ReduceResultWriter {


    private Integer numWorkers;
    private String reducerPartition;
    private String outputFileLocation;
    private FileWriter reduceWriter;

    public ReduceResultWriter(Integer numWorkers, String outputFileLocation, String reducerPartition) throws IOException {
        this.numWorkers = numWorkers;
        this.outputFileLocation = outputFileLocation;
        this.reducerPartition = reducerPartition;

        for (Integer i = 0; i< this.numWorkers; i++){
            Path outputFileFullName = Paths.get(this.outputFileLocation, "result_part_"+this.reducerPartition+".txt");
            reduceWriter = new FileWriter(outputFileFullName.toString());
        }

    }

    public void writeResult(String key, Object value) throws IOException {
        String result = key + " " + value + "\n";
        reduceWriter.write(result);
    }

    public void reduceWriterClose() throws IOException {
        reduceWriter.close();
    }
}
