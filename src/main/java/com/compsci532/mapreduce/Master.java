package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Master {
    public String masterID;
    private JobConf jobConfig;
    private Map <String, Map<String, String>> WorkerStatus = new HashMap<>();
    private Map <String, Process> processManager = new HashMap<>();
    private HeartBeatServer heartBeatServer;
    private final String heartbeatServerAddress = "//localhost:9997/HeartBeatServer";
    private final int heartBeatServerPort = 9997;
    private final long workerTimeOut = 3; //In seconds

    public Master() throws RemoteException {
        this.masterID = UUID.randomUUID().toString();

        try{
            LocateRegistry.createRegistry(this.heartBeatServerPort);
        }
        catch (ExportException e){
            LocateRegistry.getRegistry(this.heartBeatServerPort);
        }


        this.heartBeatServer = new HeartBeatServer();
    }

    public void setJobConfig(JobConf jobConfig){
        this.jobConfig = jobConfig;
    }

    public void runJob() throws IOException {

        registerHeartBeatServer();
        inputPartitioner();
        createAndRunMapperMulti();


        while(!heartBeatChecker()){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.heartBeatServer.reset();
        createAndRunReducerMulti();
        while(!heartBeatChecker()){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        deRegisterHearBeatServer();

        // Uncomment to check if workers are created and logged into status map
        //printWorkerStatus();
    }


    private boolean createAndRunMapperMulti(){

        for (int i = 0; i<this.jobConfig.numWorkers;i++){

            String thisMapperID = UUID.randomUUID().toString();
            Map<String, String> thisStatus = new HashMap<>();
            thisStatus.put("type", "Mapper");
            thisStatus.put("status", "created");
            thisStatus.put("partition", Integer.toString(i));

            ProcessBuilder mapperProcess;
            if (this.jobConfig.deliberateFailure.equals("true") && i==0){
                this.WorkerStatus.put(thisMapperID, thisStatus);
                mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker",
                        "map", this.jobConfig.MapFunc.getName(),
                        this.jobConfig.inputPartitionedFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers),
                        thisMapperID, Integer.toString(i), this.jobConfig.jobName, "true");
            }
            else{
                this.WorkerStatus.put(thisMapperID, thisStatus);
                mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker",
                        "map", this.jobConfig.MapFunc.getName(),
                        this.jobConfig.inputPartitionedFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers),
                        thisMapperID, Integer.toString(i), this.jobConfig.jobName, "false");
            }


            mapperProcess.inheritIO();

            try {
                Process mapper = mapperProcess.start();
                this.processManager.put(thisMapperID, mapper);
                //mapper.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }



        }
        return true;

    }

    private boolean createAndRunMapperOnFail(String assignedPart){
        String thisMapperID = UUID.randomUUID().toString();
        Map<String, String> thisStatus = new ConcurrentHashMap<>();
        thisStatus.put("type", "Mapper");
        thisStatus.put("status", "created");
        thisStatus.put("partition", assignedPart);

        this.WorkerStatus.put(thisMapperID, thisStatus);
        ProcessBuilder mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker",
                "map", this.jobConfig.MapFunc.getName(),
                this.jobConfig.inputPartitionedFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers),
                thisMapperID, assignedPart, this.jobConfig.jobName, "false");
        mapperProcess.inheritIO();

        try {
            Process mapper = mapperProcess.start();
            this.processManager.put(thisMapperID, mapper);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean createAndRunReducerMulti(){


        for (int i=0; i<this.jobConfig.numWorkers; i++){

            String thisReducerID = UUID.randomUUID().toString();
            Map<String, String> thisStatus = new HashMap<>();
            thisStatus.put("type", "Reducer");
            thisStatus.put("status", "created");
            thisStatus.put("partition", Integer.toString(i));
            this.WorkerStatus.put(thisReducerID, thisStatus);


            ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce",
                    "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
                    "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile,
                    Integer.toString(this.jobConfig.numWorkers), thisReducerID, Integer.toString(i), this.jobConfig.jobName, "false");
            reducerProcess.inheritIO();


            try {
                Process reducer = reducerProcess.start();
                this.processManager.put(thisReducerID, reducer);
                //reducer.waitFor(); // Master waits until this finishes execution. Not a long-term solution as programs won't be running parallely but paused
            } catch (IOException e) {
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

    private boolean heartBeatChecker(){
        LocalDateTime now = LocalDateTime.now();
        Map<String, LocalDateTime> lastActive = heartBeatServer.getLastActive();
        Map<String, String> status = heartBeatServer.getStatus();

        Integer completed = 0;
        for (Map.Entry thisStatus : status.entrySet()) {
            String key = (String)thisStatus.getKey();
            if(thisStatus.getValue().toString().equals("completed")){
                completed++;
                Map<String, String> stat = this.WorkerStatus.get(key);
                stat.put("status", "completed");
                this.WorkerStatus.put(key,stat);
            }
        }

        if(completed==this.jobConfig.numWorkers){
            return true;
        }


        for (Map.Entry thisLastActive : lastActive.entrySet()) {
            String key = (String)thisLastActive.getKey();
            LocalDateTime lastAct = (LocalDateTime) thisLastActive.getValue();

            long diff = ChronoUnit.SECONDS.between(lastAct, now);

            if (diff > this.workerTimeOut && !status.get(key).equals("completed")){
                System.out.println("This worker failed " + key);
                this.processManager.get(key).destroy();
                this.heartBeatServer.removeWorker(key);
                this.WorkerStatus.get(key).put("status", "failed");
                String type = this.WorkerStatus.get(key).get("type");
                String failedPartition = this.WorkerStatus.get(key).get("partition");

                if(type.equals("Mapper")){
                    createAndRunMapperOnFail(failedPartition);
                }
            }
        }



        return false;
    }

    private void registerHeartBeatServer(){
        try {

            Naming.rebind(this.heartbeatServerAddress, this.heartBeatServer);
            System.err.println("Server ready");

        } catch (Exception e) {

            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();

        }
    }

    private void deRegisterHearBeatServer(){
        try{
            // Unregister ourself
            Naming.unbind(this.heartbeatServerAddress);

            // Unexport; this will also remove us from the RMI runtime
            UnicastRemoteObject.unexportObject(this.heartBeatServer, true);

        }
        catch(Exception e){}
    }



}
