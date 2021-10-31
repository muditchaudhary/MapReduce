package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class Master {
    public String masterID;
    private JobConf jobConfig;
    private Map <String, Map<String, String>> WorkerStatus = new HashMap<>();

    public Master(){
        this.masterID = UUID.randomUUID().toString();
    }

    public void setJobConfig(JobConf jobConfig){
        this.jobConfig = jobConfig;
    }

    public void runJob() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {



        // MAPPER

        /*String thisMapperID = UUID.randomUUID().toString();

        ProcessBuilder mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker", "map", this.jobConfig.MapFunc.getName(),
                this.jobConfig.inputFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers), thisMapperID);
        mapperProcess.inheritIO();


        try {
            Process mapper = mapperProcess.start();
            mapper.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        createAndRunMapper();
        createAndRunReducer();


//        // Reducer
//        String thisReducerID = UUID.randomUUID().toString();
//
//        ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
//               "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile,Integer.toString(this.jobConfig.numWorkers), thisReducerID);
//        reducerProcess.inheritIO();
//
//
//        try {
//            Process reducer = reducerProcess.start();
//            reducer.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        printWorkerStatus();
    }


    private boolean createAndRunMapper(){

        String thisMapperID = UUID.randomUUID().toString();
        Map<String, String> thisStatus = new HashMap<>();
        thisStatus.put("type", "Mapper");
        thisStatus.put("status", "InProgress");
        thisStatus.put("partition", "1");

        this.WorkerStatus.put(thisMapperID, thisStatus);
        ProcessBuilder mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker", "map", this.jobConfig.MapFunc.getName(),
                this.jobConfig.inputFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers), thisMapperID);
        mapperProcess.inheritIO();


        try {
            Process mapper = mapperProcess.start();
            mapper.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean createAndRunReducer(){


        String thisReducerID = UUID.randomUUID().toString();
        Map<String, String> thisStatus = new HashMap<>();
        thisStatus.put("type", "Reducer");
        thisStatus.put("status", "InProgress");
        thisStatus.put("partition", "1");
        this.WorkerStatus.put(thisReducerID, thisStatus);

        ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
                "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile,Integer.toString(this.jobConfig.numWorkers), thisReducerID);
        reducerProcess.inheritIO();


        try {
            Process reducer = reducerProcess.start();
            reducer.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();

            return false;
        }

        return true;
    }

    private void printWorkerStatus(){

        for (Map.Entry<String, Map<String, String>> mapElement : this.WorkerStatus.entrySet()) {
            String key = (String)mapElement.getKey();

            System.out.print(key);
            System.out.println(": "+ mapElement.getValue());
//            for(Map.Entry<String, String> innerEntry : mapElement.getValue().entrySet()){
//                String key_inner = (String)mapElement.getKey();
//                //System.out.print(key_inner);
//                System.out.println(": "+ mapElement.getValue());
//
//            }
        }

    }



}
