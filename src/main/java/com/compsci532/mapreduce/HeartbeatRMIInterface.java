package com.compsci532.mapreduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.time.LocalDateTime;

public interface HeartbeatRMIInterface extends Remote {

    public void heartBeatReceiver(String workerID, LocalDateTime timestamp, String status) throws RemoteException;

}
