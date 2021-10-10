package com.compsci532.mapreduce;

import java.util.UUID;

public abstract class Worker {
    private String type;
    private String workerID;

    public Worker(String type){
        this.type = type;
        this.workerID = UUID.randomUUID().toString();
    }

    public void getDetails(){
        System.out.println("Type: "+ this.type + " | Worker ID: "+ this.workerID);
    }

}
