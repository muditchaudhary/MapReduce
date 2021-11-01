package com.compsci532.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;


public class MapResultWriter {

    private Integer numWorkers;
    private String intermediateFileLocation;
    private String mapperID;
    private ArrayList<FileWriter> WriterList= new ArrayList<>();

    public MapResultWriter(Integer numWorkers, String intermediateFileLocation, String mapperID) throws IOException {
        this.numWorkers = numWorkers;
        this.intermediateFileLocation = intermediateFileLocation;
        this.mapperID = mapperID;

        for (Integer i = 0; i< this.numWorkers; i++){
            Path intermediateFileFullName = Paths.get(this.intermediateFileLocation, this.mapperID+"_"+i+".txt");
            WriterList.add(new FileWriter(intermediateFileFullName.toString()));
        }

    }

    public void writeResult(String key, Object value) throws IOException {
        String result = key + " " + value + "\n";
        Integer key_partition = Math.abs(key.hashCode()%this.numWorkers);
        WriterList.get(key_partition).write(result);
    }

    public void mapWriterClose() throws IOException {
        for (Integer i = 0; i< this.numWorkers; i++){
            WriterList.get(i).close();
        }
    }
}
