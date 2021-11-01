package com.compsci532.mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.time.LocalDateTime;

/**
 * Heartbeat server RMI Interface
 */
public interface HeartbeatRMIInterface extends Remote {

    public void heartBeatReceiver(String workerID, LocalDateTime timestamp, String status) throws RemoteException;

}
