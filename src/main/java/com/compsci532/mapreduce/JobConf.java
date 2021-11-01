package com.compsci532.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

/**
 * Job configuration class for a job. Mainintains the file locations, job name, number of workers, Mapper .
 * Reducer functions for the job.
 */
public class JobConf {

    public String jobID;            // Job ID
    public String jobName;          // Job Name
    public String inputFile;        // Input File
    public String intermediateFile; // Intermediate file location
    public String outputFile;       // Output file location
    public Integer numWorkers;      // Number of worker
    public String inputPartitionedFile;     // Partitioned input file location
    public String deliberateFailure = "false";  // Flag for performing deliberate failure of 1 Mapper

    public Class<? extends Mapper> MapFunc;
    public Class<? extends Reducer> ReduceFunc;

    /**
     * Constructor method for job configuration
     *
     * @param jobName
     * @param configFile
     * @throws IOException
     */
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

    /**
     * Constructor method to deliberately fail a worker for testing purposes.
     *
     * @param jobName
     * @param configFile
     * @param deliberateFailure
     * @throws IOException
     */

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

    /**
     * Setter function for the Mapper function
     *
     * @param MyMapFunc
     */
    public void setMapper(Class <? extends Mapper> MyMapFunc) {
        this.MapFunc = MyMapFunc;
    }

    /**
     *  Setter function for the Reducer function
     *
     * @param ReduceFunc
     */
    public void setReducer (Class <? extends Reducer> ReduceFunc){
        this.ReduceFunc = ReduceFunc;
    }

    /**
     *
     * Setter function for the input file location which also creates the directories and sets location for the
     * partitioned input file
     *
     * @param inputFile
     * @throws IOException
     */
    public void setInputFile (String inputFile) throws IOException {

        this.inputFile = inputFile;
        Path inputPartitions = Paths.get("resources", "File_partitions",this.jobName);
        Files.createDirectories(inputPartitions);
        this.inputPartitionedFile = inputPartitions.toString();
    }

    /**
     * Setter function for the output file which creates directories for the job's output.
     *
     * @param outputFile
     * @throws IOException
     */
    public void setOutputFile (String outputFile) throws IOException {
        Path outputFileLocation = Paths.get(outputFile, this.jobName);
        this.outputFile = outputFileLocation.toString();
        Files.createDirectories(outputFileLocation);
    }

    /**
     * Setter function for the intermediate file locations which creates directories for the job's intermediate
     * outputs
     *
     * @param intermediateFile
     * @throws IOException
     */
    public void setIntermediateLocation(String intermediateFile) throws IOException {
        Path intermediateFileLocations = Paths.get(intermediateFile, this.jobName);
        this.intermediateFile = intermediateFileLocations.toString();
        Files.createDirectories(intermediateFileLocations);
    }
}
