package com.compsci532.usercode;

import com.compsci532.mapreduce.*;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Main {

    // Task 1 Word Count
    public static class WordCountMapper implements Mapper {

        public void map(String key, String value, MapResultWriter writer) throws IOException {
            String[] words = value.split(" ");

            for (String word : words){
                writer.writeResult(word, 1);
            }
        }
    }

    public static class WordCountReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException {
            int sum = 0;
            for (String val : values){
                sum+= Integer.parseInt(val);
            }

            writer.writeResult(key, sum);
        }
    }

    // Task 2 Get total sales for each date

    public static class GetTotalSalesMapper implements Mapper{
        public void map(String key, String value, MapResultWriter writer) throws IOException {
            String[] words = value.split(" ");
            String date = words[0];
            String sale = words[words.length-1];

            writer.writeResult(date, sale);

        }
    }

    public static class GetTotalSalesReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, ReduceResultWriter writer) throws IOException {
            Float sum = 0.00f;
            for (String val : values){
                sum+= Float.parseFloat(val);
            }

            writer.writeResult(key, sum);
        }
    }

    // Task 3 Calculate average price of a stock for given week prices for each day
    public static class GetAverageStockPriceMapper implements Mapper{
        public void map(String key, String value, MapResultWriter writer) throws IOException {
            String[] words = value.split(" ");
            String stock = words[1];
            String stockPrice = words[2];

            writer.writeResult(stock, stockPrice);

        }
    }

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

    //Task 4 Search if a word exists
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

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
        Path wordCountConfig = Paths.get("resources", "configs", "wordCountConfig.properties");
        JobConf wordCountJobConfig = new JobConf( "WordCount", wordCountConfig.toString());
        Master masterClient = new Master();
        System.out.println("Master ID: " + masterClient.masterID);

        System.out.println("JobConfig ID: "+ wordCountJobConfig.jobID);
        System.out.println("Running Job: " + wordCountJobConfig.jobName);
        wordCountJobConfig.setMapper(WordCountMapper.class);
        wordCountJobConfig.setReducer(WordCountReducer.class);
        masterClient.setJobConfig(wordCountJobConfig);
        masterClient.runJob();

        Path getTotalSalesConfig = Paths.get("resources", "configs", "getTotalSalesConfig.properties");
        JobConf getTotalSalesJobConfig = new JobConf( "getTotalSales", getTotalSalesConfig.toString());

        System.out.println("JobConfig ID: "+ getTotalSalesJobConfig.jobID);
        System.out.println("Running Job: " + getTotalSalesJobConfig.jobName);
        getTotalSalesJobConfig.setMapper(GetTotalSalesMapper.class);
        getTotalSalesJobConfig.setReducer(GetTotalSalesReducer.class);
        masterClient.setJobConfig(getTotalSalesJobConfig);
        //masterClient.runJob();

        Path getAverageStockPriceConfig = Paths.get("resources", "configs", "getAverageStockPriceConfig.properties");
        JobConf getAverageStockPriceJobConfig = new JobConf( "getAverageStockPrice", getAverageStockPriceConfig.toString());
        System.out.println("JobConfig ID: "+ getAverageStockPriceJobConfig.jobID);
        System.out.println("Running Job: " + getAverageStockPriceJobConfig.jobName);
        getAverageStockPriceJobConfig.setMapper(GetAverageStockPriceMapper.class);
        getAverageStockPriceJobConfig.setReducer(GetAverageStockPriceReducer.class);
        masterClient.setJobConfig(getAverageStockPriceJobConfig);
        //masterClient.runJob();

        Path searchWordConfig = Paths.get("resources", "configs", "searchWordConfig.properties");
        JobConf searchWordJobConfig = new JobConf( "searchWord", searchWordConfig.toString());
        System.out.println("JobConfig ID: "+ searchWordJobConfig.jobID);
        System.out.println("Running Job: " + searchWordJobConfig.jobName);
        searchWordJobConfig.setMapper(SearchWordMapper.class);
        searchWordJobConfig.setReducer(SearchWordReducer.class);
        masterClient.setJobConfig(searchWordJobConfig);
        //masterClient.runJob();
    }
}
