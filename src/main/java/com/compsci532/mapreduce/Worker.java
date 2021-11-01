package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Worker class
 */
public class Worker {
    private String type; // Worker type
    private String workerID;    // Worker UUID assigned by the master
    private String funcClassStr;    // Worker's function's class
    private String inputFile;   // Input File location (partitioned input for mapper)
    private String intermediateFile; // Intermediate file location
    private String outputFile;  // Output file location
    private Integer numWorkers; // Number of workers
    private String assignedPartition;   // Partition assigned to the worker (Different meanings for mapper and reducer)
    private String jobName; // Job Name
    private static HeartbeatRMIInterface look_up;   // Heartbeat server communicator
    private String deliberateFailure;   // Flag for deliberate failure of mapper

    /**
     * Constructor method for Worker
     * @param type
     * @param funcClassStr
     * @param inputFile
     * @param intermediateFile
     * @param outputFile
     * @param numWorkers
     * @param ID
     * @param assignedPartition
     * @param jobName
     * @param deliberateFailure
     * @throws MalformedURLException
     * @throws NotBoundException
     * @throws RemoteException
     */
    public Worker(String type, String funcClassStr, String inputFile, String intermediateFile, String outputFile,
                  Integer numWorkers, String ID, String assignedPartition,
                  String jobName, String deliberateFailure) throws MalformedURLException, NotBoundException, RemoteException {
        this.type = type;
        this.workerID = ID;
        this.funcClassStr = funcClassStr;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.intermediateFile = intermediateFile;
        this.numWorkers = numWorkers;
        this.assignedPartition = assignedPartition; //Works as the input partition assigned to mapper and as the input parition of the reducer
        this.jobName = jobName;
        this.deliberateFailure = deliberateFailure;
        look_up = (HeartbeatRMIInterface) Naming.lookup("//localhost:9997/HeartBeatServer");
    }

    /**
     * Get worker details method for monitoring and debugging purposes
     */
    public void getDetails(){
        System.out.println("Type: "+ this.type + " | Worker ID: "+ this.workerID);
    }


    /**
     * Execute worker's method
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InterruptedException
     */
    public void execute() throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, InterruptedException {

        if (this.type.equals("map")){
            mapProcessor();
        }
        else if (this.type.equals("reduce")){
            reduceProcessor();
        }

    }

    /**
     * Processor for reduce that performs required action before invoking reducer function
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws IOException
     * @throws InvocationTargetException
     */
    private void reduceProcessor() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IOException, InvocationTargetException {
        look_up.heartBeatReceiver(this.workerID, LocalDateTime.now(),"InProgress");
        Class reduceClass = Class.forName(this.funcClassStr);
        Object execFuncObj = reduceClass.newInstance();
        Method execFuncMethod = reduceClass.getMethod(this.type, String.class, ArrayList.class, ReduceResultWriter.class);

        ReduceResultWriter writer = new ReduceResultWriter(this.numWorkers, this.outputFile, this.assignedPartition);

        // Shuffle and group the values by key
        HashMap<String, ArrayList<String>> sortedResult = sortAndShuffle(this.intermediateFile);

        // Run reducer function
        for (Map.Entry mapElement : sortedResult.entrySet()) {
            look_up.heartBeatReceiver(this.workerID, LocalDateTime.now(),"InProgress");
            String key = (String)mapElement.getKey();
            ArrayList<String> values = (ArrayList<String>) mapElement.getValue();
            execFuncMethod.invoke(execFuncObj,key,values,writer);

        }
        writer.reduceWriterClose();

        // Send completed status heartbeat
        look_up.heartBeatReceiver(this.workerID, LocalDateTime.now(),"completed");

    }

    /**
     * Processor for mapper that performs required action before invoking mapper function
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    private void mapProcessor() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        look_up.heartBeatReceiver(this.workerID, LocalDateTime.now(),"InProgress");
        Class mapClass = Class.forName(this.funcClassStr);
        Object execFuncObj = mapClass.newInstance();
        Method execFuncMethod = mapClass.getMethod(this.type, String.class, String.class, MapResultWriter.class);

        Path inputFileLocation = Paths.get(this.inputFile, this.jobName+"_"+this.assignedPartition+".txt");
        File inputFile = new File(inputFileLocation.toString());
        Scanner myReader = new Scanner(inputFile);

        MapResultWriter mapWriter = new MapResultWriter(this.numWorkers, this.intermediateFile, this.workerID);

        // Exit if deliberateFailure is true
        if(this.deliberateFailure.equals("true")){
            System.exit(0);
        }

        // Perform mapper function
        while (myReader.hasNextLine()) {
            LocalDateTime now = LocalDateTime.now();

            look_up.heartBeatReceiver(this.workerID, now,"InProgress");

            String line = myReader.nextLine();
            execFuncMethod.invoke(execFuncObj,null,line, mapWriter);
        }
        myReader.close();
        mapWriter.mapWriterClose();
        // Send completed status heartbeat
        look_up.heartBeatReceiver(this.workerID, LocalDateTime.now(),"completed");
    }

    /**
     * Shuffle and group values according to key to be fed into reducer
     *
     * @param intermediateFile
     * @return
     * @throws FileNotFoundException
     */
    private HashMap<String, ArrayList<String>> sortAndShuffle(String intermediateFile) throws FileNotFoundException {
        HashMap<String, ArrayList<String>> sortedResult = new HashMap<>();

        File intermediateFileDir = new File(intermediateFile);
        File[] intermediateResultFiles = intermediateFileDir.listFiles((d, name) -> name.endsWith("_"+this.assignedPartition +".txt"));

        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        for (File file : intermediateResultFiles){

            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners){

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] resultSplit = line.split(" ");
                sortedResult.computeIfAbsent(resultSplit[0], k -> new ArrayList<>()).add(resultSplit[1]);
            }

        }

        return sortedResult;

    }

    /**
     * Main function for the Worker
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws NotBoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, NotBoundException, InterruptedException {
        String workerType = args[0];
        String FuncClass = args[1];
        String inputFile = (args[2] == "null")? null: args[2] ;
        String intermediateFile = (args[3] == "null")? null: args[3] ;
        String outputFile =  (args[4] == "null")? null: args[4] ;
        Integer numWorkers = Integer.parseInt(args[5]);
        String ID = args[6];
        String reducerWorkingPartition = (args[7] == "null")? null: args[7] ;
        String jobName = args[8];
        String deliberateFailure = args[9];

        Worker thisWorker = new Worker(workerType, FuncClass, inputFile, intermediateFile, outputFile, numWorkers, ID, reducerWorkingPartition, jobName, deliberateFailure);
        thisWorker.execute();
    }
}
