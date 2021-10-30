rm -rf runMapReduce
mkdir runMapReduce
javac -sourcepath src -d runMapReduce src/main/java/**/**/**/*.java
java -cp runMapReduce com.compsci532.usercode.Main