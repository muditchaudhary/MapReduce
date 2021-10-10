package com.mapreduce;

import java.util.UUID;

public class Worker {
    private String type;
    private String workerID;

    public Worker(){
        this.workerID = UUID.randomUUID().toString();
    }

    public void setWorkerType(String type){
        this.type=type;
    }

    public void getDetails(){
        System.out.println("Type: "+ this.type + " | Worker ID: "+ this.workerID);
    }
}
