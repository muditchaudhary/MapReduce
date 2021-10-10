package com.mapreduce;

import java.util.UUID;

public class JobConf {
    public String jobID;
    public String jobName;
    public String inputFile;
    public String outputFile;

    public Class<? extends Mapper> MapFunc;
    public Class<? extends Reducer> ReduceFunc;


    public JobConf(String jobName){
        this.jobID =  UUID.randomUUID().toString();
        this.jobName = jobName;
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
        this.outputFile = outputFile;
    }
}
