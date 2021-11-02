# Team EB29 Map Reduce Implementation

## Group Members
* Ayushe Gangal (SPIRE ID: 33018381)
* Pragyanta Dhal (SPIRE ID: 33069605)
* Mudit Chaudhary (SPIRE ID: 32607978)

## How to run, compile and test

To compile, run, and test you can run the given script:
```
./compile_run_test.sh
```
Alternatively, you can run the given commands in the script in command line
```bash
$ rm -rf target
$ mkdir target
$ javac -sourcepath src -d target -cp lib/junit-platform-console-standalone-1.8.1.jar:. src/main/java/**/**/**/*.java src/test/java/*.java
$ java -jar lib/junit-platform-console-standalone-1.8.1.jar --class-path target --scan-class-path
```  
If all the cases are passed, the following output should appear:
```
Running test job: ------------> WordCount
Running test job: ------------> Search Word
Running test job: ------------> Average Stock Price

Thanks for using JUnit! Support its development at https://junit.org/sponsoring

╷
├─ JUnit Jupiter ✔
└─ JUnit Vintage ✔
   └─ TestMapReduce ✔
      ├─ getAverageStockPriceTest_N_MapperReducer ✔
      ├─ searchWordConfigTest_N_MapperReducer ✔
      ├─ searchWordConfigTest_failedMapper ✔
      ├─ getAverageStockPriceTest_failedMapper ✔
      ├─ wordCountTest_SingleMapperReducer ✔
      ├─ wordCountTest_N_MapperReducer ✔
      ├─ searchWordConfigTest_SingleMapperReducer ✔
      ├─ getAverageStockPriceTest_SingleMapperReducer ✔
      └─ wordCountTest_failedMapper ✔

Test run finished after 34382 ms
[         3 containers found      ]
[         0 containers skipped    ]
[         3 containers started    ]
[         0 containers aborted    ]
[         3 containers successful ]
[         0 containers failed     ]
[         9 tests found           ]
[         0 tests skipped         ]
[         9 tests started         ]
[         0 tests aborted         ]
[         9 tests successful      ]
[         0 tests failed          ]

```

The output of the tasks after running the task will be stored in `src/test/resources/Output_files`.  

## Structure
Our MapReduce library is available in `src/main/java/com/compsci532/mapreduce`.  
The user defined functions are available in `src/main/java/com/compsci532/usercode`.

## Config format
The config files (e.g., for testing) are available in `resources/test_configs`
The config file can follow the following example:  
```properties
inputFile=resources/Input_files/getAverageStockPrice.txt
outputFileDirectory=resources/Output_files/
num_workers=1

# sparkOutputFile property is only for testing config
sparkOutputFile=spark_test_outputs/getAverageStockPrice_testOut/part-00000
```

## Test tasks
We test our mapreduce library for UDFs on the following tasks:
* __Word Count__: Given a text file, count instances of each word.
* __Average Stock Price__: Given a file with stocks and their prices on different dates, find the average for each stock.
* __Search Word__: Given a text file, check if the given word appears.

## Generate PySpark outputs for tests \(Not Required\)
We provide a python script that runs PySpark to generate test case output files for the above tasks.   
You can run it through `python generateTestOutputs.py`. However, we have already generated and saved the test case output files in the folder `spark_test_outputs`. Hence, the grader does not need to run it to test our program.

## Versions
* Java 1.8
