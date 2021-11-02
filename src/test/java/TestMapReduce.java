import com.compsci532.mapreduce.JobConf;
import com.compsci532.usercode.Main;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.compsci532.usercode.Main.runTask;
import static com.compsci532.usercode.Main.runTaskwithFailure;

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

    private static String wordCountConfig_SingleMapReduce;
    private static String searchWordConfig_SingleMapReduce;
    private static String avgStockConfig_SingleMapReduce;
    private static String wordCountConfig_N_MapReduce;
    private static String searchWordConfig_N_MapReduce;
    private static String avgStockConfig_N_MapReduce;

    private static String sparkOutputFile = "sparkOutputFile";
    private static String outputFile;

    @Before
    public void setup(){
        try {

            wordCountConfig_SingleMapReduce = Paths.get("resources", "test_configs", "test_wordCountConfig.properties").toString();
            wordCountConfig_N_MapReduce = Paths.get("resources", "test_configs", "test_wordCountConfig_N_MapReduce.properties").toString();
            wordCountprop.load(new FileInputStream(wordCountConfig_SingleMapReduce));
            wordCountSparkOutput = wordCountprop.getProperty(sparkOutputFile);

            searchWordConfig_SingleMapReduce = Paths.get("resources", "test_configs", "test_searchWordConfig.properties").toString();
            searchWordConfig_N_MapReduce = Paths.get("resources", "test_configs", "test_searchWordConfig_N_MapReduce.properties").toString();
            searchWordProp.load(new FileInputStream(searchWordConfig_SingleMapReduce));
            searchWordSparkOutput = searchWordProp.getProperty(sparkOutputFile);

            avgStockConfig_SingleMapReduce = Paths.get("resources", "test_configs", "test_getAverageStockPriceConfig.properties").toString();
            avgStockConfig_N_MapReduce = Paths.get("resources", "test_configs", "test_getAverageStockPriceConfig_N_MapReduce.properties").toString();
            avgStockProp.load(new FileInputStream(avgStockConfig_SingleMapReduce));
            avgStockSparkOutput = avgStockProp.getProperty(sparkOutputFile);

            outputFile = wordCountprop.getProperty("outputFileDirectory");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Testcase to run MapReduce on a given input file by spawning a SINGLE mapper and SINGLE reducer processes.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void wordCountTest_SingleMapperReducer() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Word_Count";
        JobConf wordCountJobConfig = new JobConf(jobName, wordCountConfig_SingleMapReduce,"false");
        System.out.println("Running test job: ------------> " + wordCountJobConfig.jobName);
        runTask(jobName, wordCountConfig_SingleMapReduce, Main.WordCountMapper.class, Main.WordCountReducer.class);

        File sparkOutputFile = new File(wordCountSparkOutput);
        File outputFileDir = new File(wordCountJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, Integer> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners){
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                        String[] parts = line.split(" ", 2);
                        if (parts.length >= 2) {
                            String key = parts[0];
                            int value = Integer.valueOf(parts[1]);
                            appOutputMap.put(key, value);
                        } else {
                            System.out.println("Malformed line " + line);
                        }
                    } catch (NumberFormatException e) {
                    e.printStackTrace();
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

    /**
     * Testcase to run MapReduce on a given input file by spawning a SINGLE mapper and SINGLE reducer processes.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void searchWordConfigTest_SingleMapperReducer() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Search_Word";
        JobConf searchWordJobConfig = new JobConf( jobName, searchWordConfig_SingleMapReduce,"false");
        System.out.println("Running test job: ------------> " + searchWordJobConfig.jobName);

        runTask(jobName, searchWordConfig_SingleMapReduce, Main.SearchWordMapper.class, Main.SearchWordReducer.class);

        File sparkOutputFile = new File(searchWordSparkOutput);
        File outputFileDir = new File(searchWordJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, String> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners) {
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        String value = parts[1];
                        appOutputMap.put(key, value);
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    /**
     * Testcase to run MapReduce on a given input file by spawning a SINGLE mapper and SINGLE reducer processes.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void getAverageStockPriceTest_SingleMapperReducer() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Average_Stock";
        JobConf avgStockJobConfig = new JobConf( jobName, avgStockConfig_SingleMapReduce,"false");
        System.out.println("Running test job: ------------> " + avgStockJobConfig.jobName);

        runTask(jobName, avgStockConfig_SingleMapReduce, Main.GetAverageStockPriceMapper.class, Main.GetAverageStockPriceReducer.class);

        File sparkOutputFile = new File(avgStockSparkOutput);
        File outputFileDir = new File(avgStockJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, String> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners) {
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        Formatter formatter = new Formatter();
                        formatter.format("%.2f", Double.valueOf(parts[1]));
                        appOutputMap.put(key, formatter.toString());
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
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

    /**
     * Testcase to run MapReduce on a given input file by spawning N mapper and reducer processes.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void wordCountTest_N_MapperReducer() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Word_Count_N_MapReduce";
        JobConf wordCountJobConfig = new JobConf(jobName, wordCountConfig_N_MapReduce,"false");
        System.out.println("Running test job: ------------> " + wordCountJobConfig.jobName);
        runTask(jobName, wordCountConfig_N_MapReduce, Main.WordCountMapper.class, Main.WordCountReducer.class);

        File sparkOutputFile = new File(wordCountSparkOutput);
        File outputFileDir = new File(wordCountJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, Integer> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners){
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        int value = Integer.valueOf(parts[1]);
                        appOutputMap.put(key, value);
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
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

    /**
     * Testcase to run MapReduce on a given input file by spawning N mapper and reducer processes.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void searchWordConfigTest_N_MapperReducer() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Search_Word_N_MapReduce";
        JobConf searchWordJobConfig = new JobConf( jobName, searchWordConfig_N_MapReduce,"false");
        System.out.println("Running test job: ------------> " + searchWordJobConfig.jobName);

        runTask(jobName, searchWordConfig_N_MapReduce, Main.SearchWordMapper.class, Main.SearchWordReducer.class);

        File sparkOutputFile = new File(searchWordSparkOutput);
        File outputFileDir = new File(searchWordJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, String> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners) {
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        String value = parts[1];
                        appOutputMap.put(key, value);
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    /**
     * Testcase to run MapReduce on a given input file by spawning N mapper and reducer processes.
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void getAverageStockPriceTest_N_MapperReducer() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Average_Stock_N_MapReduce";
        JobConf avgStockJobConfig = new JobConf( jobName, avgStockConfig_N_MapReduce,"false");
        System.out.println("Running test job: ------------> " + avgStockJobConfig.jobName);

        runTask(jobName, avgStockConfig_N_MapReduce, Main.GetAverageStockPriceMapper.class, Main.GetAverageStockPriceReducer.class);

        File sparkOutputFile = new File(avgStockSparkOutput);
        File outputFileDir = new File(avgStockJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, String> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners) {
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        Formatter formatter = new Formatter();
                        formatter.format("%.2f", Double.valueOf(parts[1]));
                        appOutputMap.put(key, formatter.toString());
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
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

    /**
     * Testcase to handle failure of a Mapper process.
     * Once the heartbeat received by master determines a Mapper had failed,
     * Master spawns a new Mapper and assign the partition it has to work on which was earlier worked by the failed Mapper
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void wordCountTest_failedMapper() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Word_Count_failedMapper";
        JobConf wordCountJobConfig = new JobConf(jobName, wordCountConfig_N_MapReduce,"true");
        System.out.println("Running test job: ------------> " + wordCountJobConfig.jobName);
        runTaskwithFailure(jobName, wordCountConfig_N_MapReduce, Main.WordCountMapper.class, Main.WordCountReducer.class);

        File sparkOutputFile = new File(wordCountSparkOutput);
        File outputFileDir = new File(wordCountJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, Integer> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners){
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        int value = Integer.valueOf(parts[1]);
                        appOutputMap.put(key, value);
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
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

    /**
     * Testcase to handle failure of a Mapper process.
     * Once the heartbeat received by master determines a Mapper had failed,
     * Master spawns a new Mapper and assign the partition it has to work on which was earlier worked by the failed Mapper
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void searchWordConfigTest_failedMapper() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Search_Word_failedMapper";
        JobConf searchWordJobConfig = new JobConf( jobName, searchWordConfig_N_MapReduce,"true");
        System.out.println("Running test job: ------------> " + searchWordJobConfig.jobName);

        runTaskwithFailure(jobName, searchWordConfig_N_MapReduce, Main.SearchWordMapper.class, Main.SearchWordReducer.class);

        File sparkOutputFile = new File(searchWordSparkOutput);
        File outputFileDir = new File(searchWordJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, String> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners) {
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        String value = parts[1];
                        appOutputMap.put(key, value);
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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

    /**
     * Testcase to handle failure of a Mapper process.
     * Once the heartbeat received by master determines a Mapper had failed,
     * Master spawns a new Mapper and assign the partition it has to work on which was earlier worked by the failed Mapper
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Test
    public void getAverageStockPriceTest_failedMapper() throws IOException, InterruptedException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {

        String jobName = "Test_Average_Stock_failedMapper";
        JobConf avgStockJobConfig = new JobConf( jobName, avgStockConfig_N_MapReduce,"true");
        System.out.println("Running test job: ------------> " + avgStockJobConfig.jobName);

        runTaskwithFailure(jobName, avgStockConfig_N_MapReduce, Main.GetAverageStockPriceMapper.class, Main.GetAverageStockPriceReducer.class);

        File sparkOutputFile = new File(avgStockSparkOutput);
        File outputFileDir = new File(avgStockJobConfig.outputFile);
        File[] intermediateResultFiles = outputFileDir.listFiles();
        ArrayList<Scanner> intermediateResultScanners= new ArrayList<>();
        Map<String, String> appOutputMap = new HashMap<>();

        for (File file : intermediateResultFiles){
            intermediateResultScanners.add(new Scanner(file));
        }

        for (Scanner scanner : intermediateResultScanners) {
            while (scanner.hasNextLine()) {
                try {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ", 2);
                    if (parts.length >= 2) {
                        String key = parts[0];
                        Formatter formatter = new Formatter();
                        formatter.format("%.2f", Double.valueOf(parts[1]));
                        appOutputMap.put(key, formatter.toString());
                    } else {
                        System.out.println("Malformed line " + line);
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
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
