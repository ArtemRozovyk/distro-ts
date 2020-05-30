package srcs.workflow.server.central;

import srcs.workflow.job.*;

import java.rmi.*;
import java.util.*;

public interface RemoteJobExecutor extends Remote {

     Map<String, Object> submitJob(Job job) throws RemoteException;
     public void registerClient(ClientRemote client) throws RemoteException;


}
