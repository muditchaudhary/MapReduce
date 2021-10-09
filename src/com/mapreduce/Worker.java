package com.mapreduce;
public class Worker {
    private String type;
    private String WorkerID;
    public Worker(String type, String WorkerID){
        this.type = type;
        this.WorkerID = WorkerID;
    }

    public void getMyType(){
        System.out.println("My type is "+ type);
        System.out.println("My ID is " + WorkerID);
    }
}
