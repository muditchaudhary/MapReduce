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
        Worker mapWorker = new Worker("map", this.jobConfig.MapFunc.getName(),
                this.jobConfig.inputFile, this.jobConfig.intermediateFile, null) ;
        mapWorker.execute(null);

        HashMap<String, ArrayList<String>> sortedResult= sortAndShuffle(jobConfig.intermediateFile); // It is just shuffled. Need to implement sort

        Worker reduceWorker = new Worker("reduce", this.jobConfig.ReduceFunc.getName(),
                null, null, this.jobConfig.outputFile);
        reduceWorker.execute(sortedResult);
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
