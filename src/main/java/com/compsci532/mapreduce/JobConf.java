package com.compsci532.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

public class JobConf {
    public String jobID;
    public String jobName;
    public String inputFile;
    public String intermediateFile;
    public String outputFile;

    public Class<? extends Mapper> MapFunc;
    public Class<? extends Reducer> ReduceFunc;


    public JobConf(String jobName, String configFile) throws IOException {
        this.jobID =  UUID.randomUUID().toString();
        this.jobName = jobName;
        Properties prop = new Properties();

        InputStream inputStream = new FileInputStream(configFile);
        prop.load(inputStream);
        setInputFile(prop.getProperty("inputFile"));
        setOutputFile(prop.getProperty("outputFileDirectory"));
        setIntermediateFile("src/main/resources/Intermediate_files");

    }

    public void setMapper(Class <? extends Mapper> MyMapFunc) {
        this.MapFunc = MyMapFunc;
    }

    public void setReducer (Class <? extends Reducer> ReduceFunc){
        this.ReduceFunc = ReduceFunc;
    }

    public void setInputFile (String inputFile){
        this.inputFile = inputFile;
    }

    public void setOutputFile (String outputFile){
        this.outputFile = outputFile+"/"+this.jobName+"_out.txt";
    }

    public void setIntermediateFile(String intermediateFile){
        this.intermediateFile = intermediateFile+"/"+this.jobName+"_inter.txt";
    }
}
