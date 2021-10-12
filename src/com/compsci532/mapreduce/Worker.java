package com.compsci532.mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class Worker {
    private String type;
    private String workerID;
    private String funcClassStr;
    private String inputFile;
    private String intermediateFile;
    private String outputFile;

    public Worker(String type, String funcClassStr, String inputFile, String intermediateFile, String outputFile){
        this.type = type;
        this.workerID = UUID.randomUUID().toString();
        this.funcClassStr = funcClassStr;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.intermediateFile = intermediateFile;

    }

    public void getDetails(){
        System.out.println("Type: "+ this.type + " | Worker ID: "+ this.workerID);
    }


    public void execute(HashMap<String, ArrayList<String>> sortedResult) throws IOException, ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {

        if (this.type.equals("map")){
            mapProcessor();
        }
        else if (this.type.equals("reduce")){
            reduceProcessor(sortedResult);
        }

    }

    private void reduceProcessor(HashMap<String, ArrayList<String>> sortedResult) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IOException, InvocationTargetException {
        Class reduceClass = Class.forName(this.funcClassStr);
        Object execFuncObj = reduceClass.newInstance();
        Method execFuncMethod = reduceClass.getMethod(this.type, String.class, ArrayList.class, FileWriter.class);

        FileWriter myWriter = new FileWriter(this.outputFile);

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
        FileWriter myWriter = new FileWriter(this.intermediateFile);

        while (myReader.hasNextLine()) {
            String line = myReader.nextLine();
            execFuncMethod.invoke(execFuncObj,null,line,myWriter);
        }
        myReader.close();
        myWriter.close();
    }
}
