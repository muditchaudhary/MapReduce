rm -rf runMapReduce
rm -rf resources/Intermediate_files/
mkdir runMapReduce
javac -sourcepath src -d runMapReduce src/main/java/**/**/**/*.java
java -cp runMapReduce com.compsci532.usercode.Main