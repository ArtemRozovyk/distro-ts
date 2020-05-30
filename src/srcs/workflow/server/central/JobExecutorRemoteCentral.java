package srcs.workflow.server.central;

import srcs.workflow.executor.*;
import srcs.workflow.job.*;
import srcs.workflow.test.*;

import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;

public class JobExecutorRemoteCentral extends JobExecutor implements ClientRemote {

    public JobExecutorRemoteCentral(Job job) {
        super(job);

    }

    @Override
    public Map<String, Object> execute() throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost");
        RemoteJobExecutor rExecService = (RemoteJobExecutor) registry.lookup("remoteExecutor");
        UnicastRemoteObject.exportObject(this, 0);
        registry.rebind("client", this);
        rExecService.registerClient(this);

        return rExecService.submitJob(job);
    }

    @Override
    public void notify(String s) throws RemoteException {
        System.out.println(s);
    }
}
