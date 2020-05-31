package srcs.workflow.server.distributed;

import srcs.workflow.graph.*;
import srcs.workflow.job.*;

import java.rmi.*;
import java.util.*;

public interface TaskTrackerRemote extends Remote {
    void registerMaster(MasterRemote masterRemote) throws RemoteException;
    void sumbitTask(Job job,String s)throws RemoteException;
    String getId()throws RemoteException;
    int getCapacity() throws RemoteException;
    int getCurrentOccupation()throws RemoteException;
    void signal(String s) throws RemoteException;
    void initLocks(Graph<String>graph)throws RemoteException;
}
