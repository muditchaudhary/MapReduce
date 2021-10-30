import com.compsci532.mapreduce.JobConf;
import com.compsci532.mapreduce.Master;
import com.compsci532.usercode.Main;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestMapReduce {
    public static void main(String[] args) throws IOException {
        Result result = JUnitCore.runClasses(TestMapReduce.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
            failure.getException().printStackTrace();
        }
    }

    Properties wordCountprop = new Properties();
    Properties avgStockProp = new Properties();
    Properties searchWordProp = new Properties();
    private static String wordCountSparkOutput;
    private static String avgStockSparkOutput;
    private static String searchWordSparkOutput;

    Master masterClient;

    @Before
    public void setup(){
        try {
            wordCountprop.load(new FileInputStream("resources/test_configs/test_wordCountConfig.properties"));
            wordCountSparkOutput = wordCountprop.getProperty("sparkOutputFile");

            avgStockProp.load(new FileInputStream("resources/test_configs/test_getAverageStockPriceConfig.properties"));
            avgStockSparkOutput = avgStockProp.getProperty("sparkOutputFile");

            searchWordProp.load(new FileInputStream("resources/test_configs/test_searchWordConfig.properties"));
            searchWordSparkOutput = searchWordProp.getProperty("sparkOutputFile");

            masterClient = new Master();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void wordCountTest() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String wordCountConfig = "resources/test_configs/test_wordCountConfig.properties";
        JobConf wordCountJobConfig = new JobConf( "Word_Count_Test", wordCountConfig);

        System.out.println("Running test job: ------------> " + wordCountJobConfig.jobName);
        wordCountJobConfig.setMapper(Main.WordCountMapper.class);
        wordCountJobConfig.setReducer(Main.WordCountReducer.class);
        masterClient.setJobConfig(wordCountJobConfig);
        masterClient.runJob();

        File appOutputFile = new File(wordCountJobConfig.outputFile);
        File sparkOutputFile = new File(wordCountSparkOutput);

        Map<String, Integer> appOutputMap = new HashMap<>();
        try (
                BufferedReader appFileReader = new BufferedReader(new FileReader(appOutputFile))) {
            String line;
            while((line =appFileReader.readLine())!=null) {
                String[] parts = line.split(" ", 2);
                if (parts.length >= 2) {
                    String key = parts[0];
                    int value = Integer.valueOf(parts[1]);
                    appOutputMap.put(key, value);
                } else {
                    System.out.println("Malformed line " + line);
                }
            }
        }

        Map<String, Integer> sparkOutputMap = new HashMap<>();
        try (
                BufferedReader sparkFileReader = new BufferedReader(new FileReader(sparkOutputFile))) {
            String line;
            while ((line = sparkFileReader.readLine()) != null) {
                String sparkOutputRegex = "\\('([a-zA-Z0-9]+)',\\s*([0-9]+)\\)";
                Pattern r = Pattern.compile(sparkOutputRegex);
                Matcher m = r.matcher(line);

                if (m.find()) {
                    sparkOutputMap.put(m.group(1), Integer.valueOf(m.group(2)));
                } else {
                    System.out.println("NO MATCH");
                }
            }
        }
        Assert.assertEquals(appOutputMap,sparkOutputMap);
    }

    @Test
    public void searchWordConfigTest() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String searchWordConfig = "resources/test_configs/test_searchWordConfig.properties";
        JobConf searchWordJobConfig = new JobConf( "Search_Word_Test", searchWordConfig);
        System.out.println("Running test job: ------------> " + searchWordJobConfig.jobName);
        searchWordJobConfig.setMapper(Main.SearchWordMapper.class);
        searchWordJobConfig.setReducer(Main.SearchWordReducer.class);
        masterClient.setJobConfig(searchWordJobConfig);
        masterClient.runJob();

        File appOutputFile = new File(searchWordJobConfig.outputFile);
        File sparkOutputFile = new File(searchWordSparkOutput);

        Map<String, String> appOutputMap = new HashMap<>();
        try (
                BufferedReader appFileReader = new BufferedReader(new FileReader(appOutputFile))) {
            String line;
            while((line =appFileReader.readLine())!=null) {
                String[] parts = line.split(" ", 2);
                if (parts.length >= 2) {
                    String key = parts[0];
                    String value = parts[1];
                    appOutputMap.put(key, value);
                } else {
                    System.out.println("Malformed line " + line);
                }
            }
        }

        Map<String, String> sparkOutputMap = new HashMap<>();
        try (
                BufferedReader sparkFileReader = new BufferedReader(new FileReader(sparkOutputFile))) {
            String line;
            while ((line = sparkFileReader.readLine()) != null) {
                String sparkOutputRegex = "\\('([a-zA-Z0-9]+)',\\s*'([a-zA-Z0-9]+)'\\)";
                Pattern r = Pattern.compile(sparkOutputRegex);
                Matcher m = r.matcher(line);

                if (m.find()) {
                    sparkOutputMap.put(m.group(1), m.group(2));
                } else {
                    System.out.println("NO MATCH");
                }
            }
        }
        Assert.assertEquals(appOutputMap,sparkOutputMap);
    }

    @Test
    public void getAverageStockPriceTest() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String avgStockConfig = "resources/test_configs/test_getAverageStockPriceConfig.properties";
        JobConf avgStockJobConfig = new JobConf( "Average_Stock_Price_Test", avgStockConfig);
        System.out.println("Running test job: ------------> " + avgStockJobConfig.jobName);
        avgStockJobConfig.setMapper(Main.GetAverageStockPriceMapper.class);
        avgStockJobConfig.setReducer(Main.GetAverageStockPriceReducer.class);
        masterClient.setJobConfig(avgStockJobConfig);
        masterClient.runJob();

        File appOutputFile = new File(avgStockJobConfig.outputFile);
        File sparkOutputFile = new File(avgStockSparkOutput);

        Map<String, String> appOutputMap = new HashMap<>();
        try (
                BufferedReader appFileReader = new BufferedReader(new FileReader(appOutputFile))) {
            String line;
            while((line =appFileReader.readLine())!=null) {
                String[] parts = line.split(" ", 2);
                if (parts.length >= 2) {
                    String key = parts[0];
                    Formatter formatter = new Formatter();
                    formatter.format("%.2f", Double.valueOf(parts[1]));
                    appOutputMap.put(key, formatter.toString());
                } else {
                    System.out.println("Malformed line " + line);
                }
            }
        }

        Map<String, String> sparkOutputMap = new HashMap<>();
        try (
                BufferedReader sparkFileReader = new BufferedReader(new FileReader(sparkOutputFile))) {
            String line;
            while ((line = sparkFileReader.readLine()) != null) {
                String sparkOutputRegex = "\\('([a-zA-Z0-9]+)',\\s*([0-9]+.[0-9]+)\\)";
                Pattern r = Pattern.compile(sparkOutputRegex);
                Matcher m = r.matcher(line);

                if (m.find()) {
                    Formatter formatter = new Formatter();
                    formatter.format("%.2f", Double.valueOf(m.group(2)));
                    sparkOutputMap.put(m.group(1), formatter.toString() );
                } else {
                    System.out.println("NO MATCH");
                }
            }
        }
        Assert.assertEquals(appOutputMap,sparkOutputMap);
    }
}
