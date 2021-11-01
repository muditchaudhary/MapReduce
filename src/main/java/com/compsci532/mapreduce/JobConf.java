package com.compsci532.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

public class JobConf {
    public String jobID;
    public String jobName;
    public String inputFile;
    public String intermediateFile;
    public String outputFile;
    public Integer numWorkers;
    public String inputPartitionedFile;
    public String deliberateFailure = "false";

    public Class<? extends Mapper> MapFunc;
    public Class<? extends Reducer> ReduceFunc;


    public JobConf(String jobName, String configFile) throws IOException {
        this.jobID =  UUID.randomUUID().toString();
        this.jobName = jobName;
        Properties prop = new Properties();

        InputStream inputStream = new FileInputStream(configFile);
        prop.load(inputStream);

        this.numWorkers = Integer.parseInt(prop.getProperty("num_workers"));
        setInputFile(prop.getProperty("inputFile"));
        setOutputFile(prop.getProperty("outputFileDirectory"));

        Path intermediateLoc = Paths.get("resources", "Intermediate_files");
        setIntermediateLocation(intermediateLoc.toString());


    }
    public JobConf(String jobName, String configFile, String deliberateFailure) throws IOException {
        this.jobID =  UUID.randomUUID().toString();
        this.jobName = jobName;
        Properties prop = new Properties();

        InputStream inputStream = new FileInputStream(configFile);
        prop.load(inputStream);

        this.numWorkers = Integer.parseInt(prop.getProperty("num_workers"));
        setInputFile(prop.getProperty("inputFile"));
        setOutputFile(prop.getProperty("outputFileDirectory"));

        Path intermediateLoc = Paths.get("resources", "Intermediate_files");
        setIntermediateLocation(intermediateLoc.toString());

        this.deliberateFailure = deliberateFailure;

    }

    public void setMapper(Class <? extends Mapper> MyMapFunc) {
        this.MapFunc = MyMapFunc;
    }

    public void setReducer (Class <? extends Reducer> ReduceFunc){
        this.ReduceFunc = ReduceFunc;
    }

    public void setInputFile (String inputFile) throws IOException {
        //Will have to be changed when dynamically creating directories per job
        this.inputFile = inputFile;
        Path inputPartitions = Paths.get("resources", "File_partitions",this.jobName);
        Files.createDirectories(inputPartitions);
        this.inputPartitionedFile = inputPartitions.toString();
    }

    public void setOutputFile (String outputFile) throws IOException {
        Path outputFileLocation = Paths.get(outputFile, this.jobName);
        this.outputFile = outputFileLocation.toString();
        Files.createDirectories(outputFileLocation);
    }

    public void setIntermediateLocation(String intermediateFile) throws IOException {

        Path intermediateFileLocations = Paths.get(intermediateFile, this.jobName);
        this.intermediateFile = intermediateFileLocations.toString();
        Files.createDirectories(intermediateFileLocations);
    }
}
