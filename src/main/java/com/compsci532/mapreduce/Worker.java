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

public class Worker {
    private String type;
    private String workerID;
    private String funcClassStr;
    private String inputFile;
    private String intermediateFile;
    private String outputFile;
    private Integer numWorkers;
    private String assignedPartition;
    private String jobName;
    private static HeartbeatRMIInterface look_up;
    private String deliberateFailure;

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

    public void getDetails(){
        System.out.println("Type: "+ this.type + " | Worker ID: "+ this.workerID);
    }


    public void execute() throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, InterruptedException {

        if (this.type.equals("map")){
            mapProcessor();
        }
        else if (this.type.equals("reduce")){
            reduceProcessor();
        }

    }

    private void reduceProcessor() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IOException, InvocationTargetException {
        Class reduceClass = Class.forName(this.funcClassStr);
        Object execFuncObj = reduceClass.newInstance();
        Method execFuncMethod = reduceClass.getMethod(this.type, String.class, ArrayList.class, ReduceResultWriter.class);

        ReduceResultWriter writer = new ReduceResultWriter(this.numWorkers, this.outputFile, this.assignedPartition);

        HashMap<String, ArrayList<String>> sortedResult = sortAndShuffle(this.intermediateFile);

        for (Map.Entry mapElement : sortedResult.entrySet()) {
            LocalDateTime now = LocalDateTime.now();
            look_up.heartBeatReceiver(this.workerID, now,"InProgress");
            String key = (String)mapElement.getKey();
            ArrayList<String> values = (ArrayList<String>) mapElement.getValue();
            execFuncMethod.invoke(execFuncObj,key,values,writer);

        }
        writer.reduceWriterClose();
        LocalDateTime now = LocalDateTime.now();
        look_up.heartBeatReceiver(this.workerID, now,"completed");

    }

    private void mapProcessor() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        look_up.heartBeatReceiver(this.workerID, LocalDateTime.now(),"InProgress");
        Class mapClass = Class.forName(this.funcClassStr);
        Object execFuncObj = mapClass.newInstance();
        Method execFuncMethod = mapClass.getMethod(this.type, String.class, String.class, MapResultWriter.class);

        Path inputFileLocation = Paths.get(this.inputFile, this.jobName+"_"+this.assignedPartition+".txt");
        File inputFile = new File(inputFileLocation.toString());
        Scanner myReader = new Scanner(inputFile);

        MapResultWriter mapWriter = new MapResultWriter(this.numWorkers, this.intermediateFile, this.workerID);

        if(this.deliberateFailure.equals("true")){
            System.exit(0);
        }
        while (myReader.hasNextLine()) {
            LocalDateTime now = LocalDateTime.now();

            look_up.heartBeatReceiver(this.workerID, now,"InProgress");

            String line = myReader.nextLine();
            execFuncMethod.invoke(execFuncObj,null,line, mapWriter);
        }
        myReader.close();
        mapWriter.mapWriterClose();
        LocalDateTime now = LocalDateTime.now();
        look_up.heartBeatReceiver(this.workerID, now,"completed");
    }

    private HashMap<String, ArrayList<String>> sortAndShuffle(String intermediateFile) throws FileNotFoundException {
        //Just shuffled until now. Need to implement sorting
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
