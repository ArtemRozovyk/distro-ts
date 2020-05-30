package srcs.workflow.server.distributed;

import srcs.workflow.job.*;

import java.rmi.*;
import java.util.*;

public interface MasterRemote extends Remote {

    void postTaskResult(String oTask, Object result) throws RemoteException;
    boolean requirementsMet(String task) throws RemoteException;
    Map<String,Object> resultsRemote()throws RemoteException;
    void submitJob(Job job) throws RemoteException;

}
