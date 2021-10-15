rm -rf target
mkdir target
javac -sourcepath src -d target -cp lib/junit-platform-console-standalone-1.8.1.jar:. src/main/java/**/**/**/*.java src/test/java/*.java
java -jar lib/junit-platform-console-standalone-1.8.1.jar --class-path target --scan-class-path