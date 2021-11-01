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

/**
 * Class for Master. Invokes Mapper and Reducer Process. Maintains mapper and reducer states.
 */
public class Master {
    public String masterID;         // UUID for the master
    private JobConf jobConfig;      // Job Configuration for the job
    private Map <String, Map<String, String>> WorkerStatus = new HashMap<>(); // Hashmap for worker state management
    private Map <String, Process> processManager = new HashMap<>(); // Hashmap for worker process management
    private HeartBeatServer heartBeatServer;    // Heartbeat RMI server for Inter Process Communication
    private final String heartbeatServerAddress = "//localhost:9997/HeartBeatServer"; // Heartbeat server Address
    private final int heartBeatServerPort = 9997;   //Heartbeat server port
    private final long workerTimeOut = 10; // Timeout to consider a worker as a failure (in seconds)


    /**
     * Constructor method for the Master
     *
     * @throws RemoteException
     */
    public Master() throws RemoteException {
        this.masterID = UUID.randomUUID().toString();

        // Set up RMI registry for the heartbeat server
        try{
            LocateRegistry.createRegistry(this.heartBeatServerPort);
        }
        catch (ExportException e){
            LocateRegistry.getRegistry(this.heartBeatServerPort);
        }

        this.heartBeatServer = new HeartBeatServer();
    }

    /**
     * Setter function for the job configuration
     *
     * @param jobConfig
     */
    public void setJobConfig(JobConf jobConfig){
        this.jobConfig = jobConfig;
    }


    /**
     * Run job function that the user uses to run the Master and invoke the worker process
     * @throws IOException
     */
    public void runJob() throws IOException {


        registerHeartBeatServer(); // Register heartbeat server
        inputPartitioner();     // Partition the input files
        createAndRunMapperMulti(); // Create and run N Mapper processes


        // Check mapper status every second and continue if all mappers have completed processing
        while(!heartBeatChecker()){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        this.heartBeatServer.reset();   // Prepare heartbeat server for reducer's heartbeat messages

        createAndRunReducerMulti(); // Create and run N reducer processes

        // Check redicer status every second and continue if all reducer have completed processing
        while(!heartBeatChecker()){
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        deRegisterHearBeatServer(); // Deregister heartbeat server

        // Uncomment to check if workers are created and logged into status map
        //printWorkerStatus();
    }

    /**
     * Convenience function to create and run N mapper process
     * @return
     */
    private boolean createAndRunMapperMulti(){

        for (int i = 0; i<this.jobConfig.numWorkers;i++){

            // Set mapper ID and other status
            String thisMapperID = UUID.randomUUID().toString();
            Map<String, String> thisStatus = new HashMap<>();
            thisStatus.put("type", "Mapper");
            thisStatus.put("status", "created");
            thisStatus.put("partition", Integer.toString(i));

            ProcessBuilder mapperProcess;

            // Process build for deliberately failing 1 mapper
            if (this.jobConfig.deliberateFailure.equals("true") && i==0){

                this.WorkerStatus.put(thisMapperID, thisStatus);

                // Build mapper process
                mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker",
                        "map", this.jobConfig.MapFunc.getName(),
                        this.jobConfig.inputPartitionedFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers),
                        thisMapperID, Integer.toString(i), this.jobConfig.jobName, "true");
            }
            // Process build for normal worker
            else{
                this.WorkerStatus.put(thisMapperID, thisStatus);

                // Build mapper process
                mapperProcess = new ProcessBuilder("java", "-cp", "runMapReduce", "com.compsci532.mapreduce.Worker",
                        "map", this.jobConfig.MapFunc.getName(),
                        this.jobConfig.inputPartitionedFile, this.jobConfig.intermediateFile, "null", Integer.toString(this.jobConfig.numWorkers),
                        thisMapperID, Integer.toString(i), this.jobConfig.jobName, "false");
            }

            // Inherit mapper processe's standard I/O for monitoring and debug messages
            mapperProcess.inheritIO();

            try {

                // Create and run mapper process
                Process mapper = mapperProcess.start();
                this.processManager.put(thisMapperID, mapper);

            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Convenience method to start a new mapper worker upon failure
     *
     * @param assignedPart
     * @return
     */
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

    /**
     * Convenience method to create and run N reducer processes
     * @return
     */
    private boolean createAndRunReducerMulti(){


        for (int i=0; i<this.jobConfig.numWorkers; i++){

            // Set reducer UUID and other status
            String thisReducerID = UUID.randomUUID().toString();
            Map<String, String> thisStatus = new HashMap<>();
            thisStatus.put("type", "Reducer");
            thisStatus.put("status", "created");
            thisStatus.put("partition", Integer.toString(i));
            this.WorkerStatus.put(thisReducerID, thisStatus);

            // Build reducer process
            ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce",
                    "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
                    "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile,
                    Integer.toString(this.jobConfig.numWorkers), thisReducerID, Integer.toString(i), this.jobConfig.jobName, "false");
            reducerProcess.inheritIO();


            try {
                // Create and run reducer worker process
                Process reducer = reducerProcess.start();
                this.processManager.put(thisReducerID, reducer);

            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }

        }

        return true;
    }

    /**
     * Method to start new reducer process on failure
     *
     * @param assignedPart
     * @return
     */
    private boolean createAndRunReducerOnFail(String assignedPart){

        // Set reducer UUID and other status
        String thisReducerID = UUID.randomUUID().toString();
        Map<String, String> thisStatus = new HashMap<>();
        thisStatus.put("type", "Reducer");
        thisStatus.put("status", "created");
        thisStatus.put("partition", assignedPart);
        this.WorkerStatus.put(thisReducerID, thisStatus);

        // Build reducer process
        ProcessBuilder reducerProcess = new ProcessBuilder("java", "-cp", "runMapReduce",
                "com.compsci532.mapreduce.Worker", "reduce", this.jobConfig.ReduceFunc.getName(),
                "null", this.jobConfig.intermediateFile, this.jobConfig.outputFile,
                Integer.toString(this.jobConfig.numWorkers), thisReducerID, assignedPart, this.jobConfig.jobName, "false");
        reducerProcess.inheritIO();


        try {
            // Create and run reducer worker process
            Process reducer = reducerProcess.start();
            this.processManager.put(thisReducerID, reducer);

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;

    }

    /**
     * Method to monitor worker status. For monitoring and debugging purposes
     */
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

    /**
     * Input file partitioner. Partitions input into num_workers partitions.
     * @throws IOException
     */
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
            WriterList.get(lines%this.jobConfig.numWorkers).write(line+"\n"); // Assign line to each partition based on line_number % num_workers
            lines++;
        }

        for (Integer i = 0; i< this.jobConfig.numWorkers; i++){
            WriterList.get(i).close();
        }

    }

    /**
     * Method to check the heartbeat messages from the workers and handling process completion
     * @return boolean: true if all worker complete, false otherwise
     */
    private boolean heartBeatChecker(){
        LocalDateTime now = LocalDateTime.now();
        Map<String, LocalDateTime> lastActive = heartBeatServer.getLastActive();
        Map<String, String> status = heartBeatServer.getStatus();


        // Check if all the workers have completed their jobs
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

        // Check if any worker is failed or not responding. If not responding for more than timeout, create a new worker
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
                if(type.equals("Reducer")){
                    createAndRunReducerOnFail(failedPartition);
                }
            }
        }

        return false;
    }

    /**
     * Register Heartbeat Server
     */
    private void registerHeartBeatServer(){
        try {

            Naming.rebind(this.heartbeatServerAddress, this.heartBeatServer);
            System.err.println("Heartbeat Server ready for "+ this.jobConfig.jobName);

        } catch (Exception e) {

            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();

        }
    }

    /**
     * Deregister Heartbeat Server
     */
    private void deRegisterHearBeatServer(){
        try{

            Naming.unbind(this.heartbeatServerAddress);

            UnicastRemoteObject.unexportObject(this.heartBeatServer, true);

        }
        catch(Exception e){}
    }



}
