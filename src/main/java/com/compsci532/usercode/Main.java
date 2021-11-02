package com.compsci532.usercode;

import com.compsci532.mapreduce.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * User's code
 */
public class Main {


    /**
     *  User defined Mapper function for Word Count Task
     */
    public static class WordCountMapper implements Mapper {

        public void map(String key, String value, MapResultWriter writer) throws IOException {
            String[] words = value.split(" ");

            for (String word : words){
                writer.writeResult(word, 1);
            }
        }
    }

    /**
     *  User defined Reducer function for Word Count Task
     */
    public static class WordCountReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException {
            int sum = 0;
            for (String val : values){
                sum+= Integer.parseInt(val);
            }

            writer.writeResult(key, sum);
        }
    }


    /**
     *  User defined Mapper function for Get average stock price task
     */
    public static class GetAverageStockPriceMapper implements Mapper{
        public void map(String key, String value, MapResultWriter writer) throws IOException {
            String[] words = value.split(" ");
            String stock = words[1];
            String stockPrice = words[2];

            writer.writeResult(stock, stockPrice);

        }
    }

    /**
     *  User defined Reducer function for Get average stock price task
     */
    public static class GetAverageStockPriceReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException {
            Float sum = 0.00f;
            int totalPrices = 0;
            for (String val : values){
                sum+= Float.parseFloat(val);
                totalPrices+=1;
            }
            Float average = sum/totalPrices;
            writer.writeResult(key, average);
        }
    }

    /**
     *  User defined Mapper function for search word task
     */
    public static class SearchWordMapper implements Mapper {
        public void map(String key, String value, MapResultWriter writer) throws IOException {
            String[] words = value.split(" ");
            String searchWord = "searchMe";

            for (String word : words) {
                String write_result = "";
                if (word.equals(searchWord)) {
                    // 1 for true; 0 for false
                    writer.writeResult(searchWord, 1);

                } else {
                    writer.writeResult(searchWord, 0);
                }

            }
        }
    }

    /**
     *  User defined Reducer function for search word task
     */
    public static class SearchWordReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException {
            int psuedoOr = 0;
            for (String val : values){
                psuedoOr+= Integer.parseInt(val);
            }
            String writeResult = "";
            if(psuedoOr > 0){
                writer.writeResult(key, "True");
            }
            else{
                writer.writeResult(key, "False");
            }

        }
    }

    /**
     * Convenience method for user to run a task
     * @param jobName
     * @param config
     * @param MapperCls
     * @param ReducerCls
     * @throws IOException
     */
    public static void runTask(String jobName, String config, Class <? extends Mapper> MapperCls, Class<? extends Reducer> ReducerCls) throws IOException {

        JobConf wordCountJobConfig = new JobConf( jobName, config); // Load job config
        Master masterClient = new Master(); // Start Master
        System.out.println("Master ID: " + masterClient.masterID);

        System.out.println("JobConfig ID: "+ wordCountJobConfig.jobID);
        System.out.println("Running Job: " + wordCountJobConfig.jobName);
        wordCountJobConfig.setMapper(MapperCls);   // Set UDF Mapper function in job config
        wordCountJobConfig.setReducer(ReducerCls);  // Set UDF Reducer function in job config
        masterClient.setJobConfig(wordCountJobConfig);  // Set Job config for the master
        masterClient.runJob();  // Master runs job based on job config

    }

    /**
     * Convenience method to test by deliberating failing a worker
     * @param jobName
     * @param config
     * @param MapperCls
     * @param ReducerCls
     * @throws IOException
     */
    public static void runTaskwithFailure(String jobName, String config, Class <? extends Mapper> MapperCls, Class<? extends Reducer> ReducerCls) throws IOException {

        JobConf wordCountJobConfig = new JobConf( jobName, config, "true");
        Master masterClient = new Master();
        System.out.println("Master ID: " + masterClient.masterID);

        System.out.println("JobConfig ID: "+ wordCountJobConfig.jobID);
        System.out.println("Running Job: " + wordCountJobConfig.jobName);
        wordCountJobConfig.setMapper(MapperCls);
        wordCountJobConfig.setReducer(ReducerCls);
        masterClient.setJobConfig(wordCountJobConfig);
        masterClient.runJob();

    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {

        // Run Word Count
        String wordCountConfig = Paths.get("resources", "configs", "wordCountConfig.properties").toString();
        runTask("WordCount", wordCountConfig, WordCountMapper.class, WordCountReducer.class);

        // Run Word Count with failure
        runTaskwithFailure("WordCountFailure", wordCountConfig, WordCountMapper.class, WordCountReducer.class);

        // Run get Average Stock Price
        String getAverageStockPriceConfig = Paths.get("resources", "configs", "getAverageStockPriceConfig.properties").toString();
        runTask("getAverageStockPrice", getAverageStockPriceConfig, GetAverageStockPriceMapper.class, GetAverageStockPriceReducer.class);

        // Run get Average Stock Price with failure
        runTaskwithFailure("getAverageStockPriceFailure", getAverageStockPriceConfig, GetAverageStockPriceMapper.class, GetAverageStockPriceReducer.class);

        // Run search word
        String searchWordConfig = Paths.get("resources", "configs", "searchWordConfig.properties").toString();
        runTask("searchWord", searchWordConfig, SearchWordMapper.class, SearchWordReducer.class);

        // Run search word with failure
        runTaskwithFailure("searchWordFailure", searchWordConfig, SearchWordMapper.class, SearchWordReducer.class);

    }
}
