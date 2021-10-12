package com.compsci532.mapreduce;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class Main {

    // Task 1 Word Count
    public static class WordCountMapper implements Mapper {

        public void map(String key, String value, FileWriter result) throws IOException {
            String[] words = value.split(" ");

            for (String word : words){
                String write_result = Mapper.mapResult(word, 1);
                result.write(write_result);
            }
        }
    }

    public static class WordCountReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, FileWriter result) throws IOException {
            int sum = 0;
            for (String val : values){
                sum+= Integer.parseInt(val);
            }

            String write_result = Reducer.reduceResult(key, sum);
            result.write(write_result
            );
        }
    }

    // Task 2 Get total sales for each date

    public static class GetTotalSalesMapper implements Mapper{
        public void map(String key, String value, FileWriter result) throws IOException {
            String[] words = value.split(" ");
            String date = words[0];
            String sale = words[words.length-1];

            String write_result = Mapper.mapResult(date, sale);
            result.write(write_result);

        }
    }

    public static class GetTotalSalesReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, FileWriter result) throws IOException {
            Float sum = 0.00f;
            for (String val : values){
                sum+= Float.parseFloat(val);
            }

            String write_result = Reducer.reduceResult(key, sum);
            result.write(write_result);
        }
    }

    // Task 3 Calculate average price of a stock for given week prices for each day
    public static class GetAverageStockPriceMapper implements Mapper{
        public void map(String key, String value, FileWriter result) throws IOException {
            String[] words = value.split(" ");
            String stock = words[1];
            String stockPrice = words[2];

            String write_result = Mapper.mapResult(stock, stockPrice);
            result.write(write_result);

        }
    }

    public static class GetAverageStockPriceReducer implements Reducer {

        public void reduce(String key, ArrayList<String> values, FileWriter result) throws IOException {
            Float sum = 0.00f;
            int totalPrices = 0;
            for (String val : values){
                sum+= Float.parseFloat(val);
                totalPrices+=1;
            }
            Float average = sum/totalPrices;
            String write_result = Reducer.reduceResult(key, average);
            result.write(write_result);
        }
    }


    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
        String wordCountConfig = "configs/wordCountConfig.properties";
        JobConf wordCountJobConfig = new JobConf( "WordCount", wordCountConfig);
        Master MasterClient = new Master();
        System.out.println("Master ID: " + MasterClient.masterID);

        System.out.println("JobConfig ID: "+ wordCountJobConfig.jobID);
        System.out.println("Running Job: " + wordCountJobConfig.jobName);
        wordCountJobConfig.setMapper(WordCountMapper.class);
        wordCountJobConfig.setReducer(WordCountReducer.class);
        MasterClient.setJobConfig(wordCountJobConfig);
        MasterClient.runJob();

        String getTotalSalesConfig = "configs/getTotalSalesConfig.properties";
        JobConf getTotalSalesJobConfig = new JobConf( "getTotalSales", getTotalSalesConfig);

        System.out.println("JobConfig ID: "+ getTotalSalesJobConfig.jobID);
        System.out.println("Running Job: " + getTotalSalesJobConfig.jobName);
        getTotalSalesJobConfig.setMapper(GetTotalSalesMapper.class);
        getTotalSalesJobConfig.setReducer(GetTotalSalesReducer.class);
        MasterClient.setJobConfig(getTotalSalesJobConfig);
        MasterClient.runJob();

        String getAverageStockPriceConfig = "configs/getAverageStockPriceConfig.properties";
        JobConf getAverageStockPriceJobConfig = new JobConf( "getAverageStockPrice", getAverageStockPriceConfig);
        System.out.println("JobConfig ID: "+ getAverageStockPriceJobConfig.jobID);
        System.out.println("Running Job: " + getAverageStockPriceJobConfig.jobName);
        getAverageStockPriceJobConfig.setMapper(GetAverageStockPriceMapper.class);
        getAverageStockPriceJobConfig.setReducer(GetAverageStockPriceReducer.class);
        MasterClient.setJobConfig(getAverageStockPriceJobConfig);
        MasterClient.runJob();
    }
}
