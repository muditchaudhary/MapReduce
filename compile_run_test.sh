rm -rf runMapReduce
mkdir runMapReduce
rm -rf resources/Intermediate_files/
javac -sourcepath src -d runMapReduce -cp lib/junit-platform-console-standalone-1.8.1.jar:. src/main/java/**/**/**/*.java src/test/java/*.java
java -jar lib/junit-platform-console-standalone-1.8.1.jar --class-path runMapReduce --scan-class-path
rm -rf runMapReduce
