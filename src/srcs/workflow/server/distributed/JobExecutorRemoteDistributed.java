package srcs.workflow.server.distributed;

import srcs.workflow.executor.*;
import srcs.workflow.job.*;
import srcs.workflow.server.central.*;
import srcs.workflow.test.*;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;

public class JobExecutorRemoteDistributed extends JobExecutor implements RemoteJobDistributedExecutor{


    public JobExecutorRemoteDistributed(Job job) {
        super(job);
    }
    final Object resultsReady= new Object();



    @Override
    public Map<String, Object> execute() throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost");
        MasterRemote masterRemote = (MasterRemote) registry.lookup("masterRemote");
        UnicastRemoteObject.exportObject(this, 0);
        registry.rebind("notUsed", this);
        masterRemote.submitJob(this,job);
        synchronized (resultsReady){
            resultsReady.wait();
            return  results;
        }
    }

    Map<String, Object> results;
    @Override
    public void notifyDoneJob(Map<String, Object> res) throws RemoteException {
        synchronized (resultsReady){
            results=res;
            resultsReady.notify();
        }
    }
}
