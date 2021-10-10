package com.mapreduce;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

public class Master {
    String masterID;
    JobConf jobConfig;

    public Master(){
        this.masterID = UUID.randomUUID().toString();
    }

    public void setJobConfig(JobConf jobConfig){
        this.jobConfig = jobConfig;
    }

    public void runJob() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException, IOException {
        runMapper();
        HashMap<String, ArrayList<String>> sortedResult= sortAndShuffle(jobConfig.intermediateFile); // It is just shuffled. Need to implement sort

        Object reducer = createReducer();
        Method reducerMethod = this.jobConfig.ReduceFunc.getMethod("Reduce");
        reducerMethod.invoke(reducer);
    }

    private Object createMapper() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> MapperCls = this.jobConfig.MapFunc;
        Object thisMapper = MapperCls.newInstance();
        Method mapperSetType = this.jobConfig.MapFunc.getMethod("setWorkerType", String.class);
        mapperSetType.invoke(thisMapper, "Mapper");
        return thisMapper;
    }

    private Object createReducer() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        Class <?> ReducerCls = this.jobConfig.ReduceFunc;
        Object thisReducer = ReducerCls.newInstance();
        return thisReducer;
    }

    private void runMapper() throws InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException, IOException {
        Object mapper = createMapper();

        File inputFile = new File(this.jobConfig.inputFile);
        Scanner myReader = new Scanner(inputFile);


        FileWriter myWriter = new FileWriter(this.jobConfig.intermediateFile);

        Method mapperMethod = this.jobConfig.MapFunc.getMethod("Map", String.class, FileWriter.class);

        while (myReader.hasNextLine()) {
            String line = myReader.nextLine();
            mapperMethod.invoke(mapper, line, myWriter);
        }
        myReader.close();
        myWriter.close();

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
        /*
        for (Map.Entry mapElement : sortedResult.entrySet()) {
            String key = (String)mapElement.getKey();

            // Add some bonus marks
            // to all the students and print it
            System.out.println(key);
            System.out.println(mapElement.getValue().toString());

        }*/

        return sortedResult;

    }

}
