package com.compsci532.mapreduce;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Heartbeat Server class
 */
public class HeartBeatServer extends UnicastRemoteObject implements HeartbeatRMIInterface {

    private Map <String, LocalDateTime> lastActive = new ConcurrentHashMap<>(); // Stores lastActive timestamp for worker
    private Map <String, String> status = new ConcurrentHashMap<>(); //Stores status of worker
    protected HeartBeatServer() throws RemoteException {
        super();

    }

    /**
     * Hearbeat receiver function
     * @param workerID
     * @param timestamp
     * @param status
     * @throws RemoteException
     */
    @Override
    public void heartBeatReceiver(String workerID, LocalDateTime timestamp, String status) throws RemoteException {
        this.lastActive.put(workerID,timestamp);
        this.status.put(workerID,status);
    }

    /**
     * Get lastActive for all workers
     * @return
     */
    public Map<String, LocalDateTime> getLastActive(){
        return this.lastActive;
    }

    /**
     * Get status for all workers
     * @return
     */
    public Map<String, String> getStatus(){
        return this.status;
    }

    /**
     * Remove worker from management
     * @param workerID
     */
    public void removeWorker(String workerID){
        this.lastActive.remove(workerID);
        this.status.remove(workerID);
    }

    /**
     * Reset lastActive and status maps
     */
    public void reset(){
        this.status.clear();
        this.lastActive.clear();
    }

}
