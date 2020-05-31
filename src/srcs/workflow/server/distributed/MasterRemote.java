package srcs.workflow.server.distributed;

import srcs.workflow.job.*;

import java.rmi.*;
import java.util.*;

public interface MasterRemote extends Remote {

    void postTaskResult(String oTask, Object result) throws RemoteException;
    boolean requirementsMet(String task) throws RemoteException;
    Map<String,Object> resultsRemote()throws RemoteException;
    void reset()throws RemoteException;
    void submitJob(RemoteJobDistributedExecutor executor,Job job) throws RemoteException;
    void registerExecutor(RemoteJobDistributedExecutor executor)throws RemoteException;
    boolean doneJob() throws RemoteException;
}
