package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class Master {
    public String masterID;
    JobConf jobConfig;

    public Master(){
        this.masterID = UUID.randomUUID().toString();
    }

    public void setJobConfig(JobConf jobConfig){
        this.jobConfig = jobConfig;
    }

    public void runJob() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {

        // MAPPER
        ProcessBuilder mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker", "map", this.jobConfig.MapFunc.getName(),
                this.jobConfig.inputFile, this.jobConfig.intermediateFile, "null");
        mapperProcess.inheritIO();


        try {
            Process mapper = mapperProcess.start();
            mapper.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // Reducer
        ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
               "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile);
        reducerProcess.inheritIO();


        try {
            Process reducer = reducerProcess.start();
            reducer.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


    private HashMap<String, ArrayList<String>> sortAndShuffle(String intermediateFile) throws FileNotFoundException {
        //Just shuffled until now. Need to implement sorting
        HashMap<String, ArrayList<String>> sortedResult = new HashMap<>();
        File intermediateResultFile = new File(this.jobConfig.intermediateFile);
        Scanner intermediateResult = new Scanner(intermediateResultFile);

        while (intermediateResult.hasNextLine()) {
            String line = intermediateResult.nextLine();
            String[] resultSplit = line.split(" ");
            sortedResult.computeIfAbsent(resultSplit[0], k -> new ArrayList<>()).add(resultSplit[1]);
        }

        // Uncomment to check content
//
//        for (Map.Entry mapElement : sortedResult.entrySet()) {
//            String key = (String)mapElement.getKey();
//
//            System.out.println(key);
//            System.out.println(mapElement.getValue().toString());
//
//        }

        return sortedResult;

    }

}
