rm -rf runMapReduceTest
mkdir runMapReduceTest
javac -sourcepath src -d runMapReduceTest -cp lib/junit-platform-console-standalone-1.8.1.jar:. src/main/java/**/**/**/*.java src/test/java/*.java
java -jar lib/junit-platform-console-standalone-1.8.1.jar --class-path runMapReduceTest --scan-class-path
rm -rf runMapReduceTest
