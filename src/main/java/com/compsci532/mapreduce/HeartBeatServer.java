package com.compsci532.mapreduce;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeartBeatServer extends UnicastRemoteObject implements HeartbeatRMIInterface {

    private Map <String, LocalDateTime> lastActive = new ConcurrentHashMap<>();
    private Map <String, String> status = new ConcurrentHashMap<>();
    protected HeartBeatServer() throws RemoteException {
        super();

    }

    @Override
    public void heartBeatReceiver(String workerID, LocalDateTime timestamp, String status) throws RemoteException {
        this.lastActive.put(workerID,timestamp);
        this.status.put(workerID,status);
    }

    public Map<String, LocalDateTime> getLastActive(){
        return this.lastActive;
    }

    public Map<String, String> getStatus(){
        return this.status;
    }

    public void removeWorker(String workerID){
        this.lastActive.remove(workerID);
        this.status.remove(workerID);
    }

    public void reset(){
        this.status.clear();
        this.lastActive.clear();
    }

}
