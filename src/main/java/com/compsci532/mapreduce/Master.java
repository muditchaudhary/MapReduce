package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    public void runJob() throws IOException {

        inputPartitioner();
        createAndRunMapper();
        createAndRunReducer();

        // Uncomment to check if workers are created and logged into status map
        //printWorkerStatus();
    }


    private boolean createAndRunMapper(){

        for (int i = 0; i<this.jobConfig.numWorkers;i++){

            String thisMapperID = UUID.randomUUID().toString();
            Map<String, String> thisStatus = new HashMap<>();
            thisStatus.put("type", "Mapper");
            thisStatus.put("status", "InProgress");
            thisStatus.put("partition", Integer.toString(i));

            this.WorkerStatus.put(thisMapperID, thisStatus);
            ProcessBuilder mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker",
                    "map", this.jobConfig.MapFunc.getName(),
                    this.jobConfig.inputPartitionedFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers),
                    thisMapperID, Integer.toString(i), this.jobConfig.jobName);
            mapperProcess.inheritIO();


            try {
                Process mapper = mapperProcess.start();
                mapper.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                return false;
            }



        }
        return true;

    }

    private boolean createAndRunReducer(){


        for (int i=0; i<this.jobConfig.numWorkers; i++){

            String thisReducerID = UUID.randomUUID().toString();
            Map<String, String> thisStatus = new HashMap<>();
            thisStatus.put("type", "Reducer");
            thisStatus.put("status", "InProgress");
            thisStatus.put("partition", Integer.toString(i));
            this.WorkerStatus.put(thisReducerID, thisStatus);


            ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce",
                    "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
                    "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile,
                    Integer.toString(this.jobConfig.numWorkers), thisReducerID, Integer.toString(i), this.jobConfig.jobName);
            reducerProcess.inheritIO();


            try {
                Process reducer = reducerProcess.start();
                reducer.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
                return false;
            }

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

    private void inputPartitioner() throws IOException {

        ArrayList<FileWriter> WriterList= new ArrayList<>();
        for (Integer i = 0; i<this.jobConfig.numWorkers; i++){
            Path intermediateFileFullName = Paths.get(this.jobConfig.inputPartitionedFile,
                    this.jobConfig.jobName+"_"+i+".txt");
            WriterList.add(new FileWriter(intermediateFileFullName.toString()));
        }

        File inputFile = new File(this.jobConfig.inputFile);
        Scanner myReader = new Scanner(inputFile);

        int lines = 0;
        while (myReader.hasNextLine()) {
            String line = myReader.nextLine();
            WriterList.get(lines%this.jobConfig.numWorkers).write(line+"\n");
            lines++;
        }

        for (Integer i = 0; i< this.jobConfig.numWorkers; i++){
            WriterList.get(i).close();
        }

    }

}
