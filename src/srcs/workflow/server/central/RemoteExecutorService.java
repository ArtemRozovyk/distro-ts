package srcs.workflow.server.central;

import srcs.workflow.executor.*;
import srcs.workflow.job.*;

import java.rmi.*;
import java.util.*;
import java.util.concurrent.*;

public class RemoteExecutorService implements RemoteJobExecutor {


    JobExecutorParallel executorParallel;
    int number;
    ClientRemote clientRemote;
    @Override
    public Map<String, Object> submitJob(Job job) throws RemoteException {
        System.out.println("Accepted job for execution : "+job.getName());
        executorParallel=new JobExecutorParallel(job);
        try {
            Map<String, Object> stringObjectMap=executorParallel.execute();
            System.out.println("Succesfully executed "+job.getName());
            clientRemote.notify(stringObjectMap.keySet().size()+"");
            return stringObjectMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RemoteException("remote executor failed to do the job"+job.getName());
    }

    @Override
    public void registerClient(ClientRemote client) throws RemoteException {
        this.clientRemote=client;
    }


}
