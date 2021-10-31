package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Worker {
    private String type;
    private String workerID;
    private String funcClassStr;
    private String inputFile;
    private String intermediateFile;
    private String outputFile;
    private Integer numWorkers;

    public Worker(String type, String funcClassStr, String inputFile, String intermediateFile, String outputFile, Integer numWorkers, String ID){
        this.type = type;
        this.workerID = ID;
        this.funcClassStr = funcClassStr;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.intermediateFile = intermediateFile;
        this.numWorkers = numWorkers;

    }

    public void getDetails(){
        System.out.println("Type: "+ this.type + " | Worker ID: "+ this.workerID);
    }


    public void execute() throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

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
        Method execFuncMethod = reduceClass.getMethod(this.type, String.class, ArrayList.class, FileWriter.class);

        FileWriter myWriter = new FileWriter(this.outputFile);

        HashMap<String, ArrayList<String>> sortedResult = sortAndShuffle(this.intermediateFile);

        for (Map.Entry mapElement : sortedResult.entrySet()) {
            String key = (String)mapElement.getKey();
            ArrayList<String> values = (ArrayList<String>) mapElement.getValue();
            execFuncMethod.invoke(execFuncObj,key,values,myWriter);

        }
        myWriter.close();
    }

    private void mapProcessor() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class mapClass = Class.forName(this.funcClassStr);
        Object execFuncObj = mapClass.newInstance();
        Method execFuncMethod = mapClass.getMethod(this.type, String.class, String.class, FileWriter.class);

        File inputFile = new File(this.inputFile);
        Scanner myReader = new Scanner(inputFile);

        ArrayList<FileWriter> WriterList= new ArrayList<>();
        for (Integer i = 0; i< this.numWorkers; i++){
            //String intermediateFileFullName = this.intermediateFile+ "/" + this.workerID + "_" + i +".txt";
            Path intermediateFileFullName = Paths.get(this.intermediateFile, i+".txt");
            WriterList.add(new FileWriter(intermediateFileFullName.toString()));
        }

        while (myReader.hasNextLine()) {
            String line = myReader.nextLine();
            execFuncMethod.invoke(execFuncObj,null,line,WriterList.get(0));
        }
        myReader.close();
        for (Integer i = 0; i< this.numWorkers; i++){
            WriterList.get(i).close();
        }
    }

    private HashMap<String, ArrayList<String>> sortAndShuffle(String intermediateFile) throws FileNotFoundException {
        //Just shuffled until now. Need to implement sorting
        HashMap<String, ArrayList<String>> sortedResult = new HashMap<>();

        Integer i = 0; //For temporary purpose only
        Path intermediateFileFullName = Paths.get(this.intermediateFile, i+".txt");
        File intermediateResultFile = new File(intermediateFileFullName.toString());
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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        String workerType = args[0];
        String FuncClass = args[1];
        String inputFile = (args[2] == "null")? null: args[2] ;
        String intermediateFile = (args[3] == "null")? null: args[3] ;
        String outputFile =  (args[4] == "null")? null: args[4] ;
        Integer numWorkers = Integer.parseInt(args[5]);
        String ID = args[6];


        Worker thisWorker = new Worker(workerType, FuncClass, inputFile, intermediateFile, outputFile, numWorkers, ID);
        thisWorker.execute();
        System.out.println("Function ran successfully");
    }
}
