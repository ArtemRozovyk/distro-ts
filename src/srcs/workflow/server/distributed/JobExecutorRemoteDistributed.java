package srcs.workflow.server.distributed;

import srcs.workflow.executor.*;
import srcs.workflow.job.*;
import srcs.workflow.server.central.*;
import srcs.workflow.test.*;

import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;

public class JobExecutorRemoteDistributed extends JobExecutor {


    public JobExecutorRemoteDistributed(Job job) {
        super(job);
    }

    @Override
    public Map<String, Object> execute() throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost");
        MasterRemote masterRemote = (MasterRemote) registry.lookup("masterRemote");
        Thread.sleep(3000);
        masterRemote.submitJob(job);
        Thread.sleep(5000);





        Map<String, Object> results =masterRemote.resultsRemote();
        masterRemote.reset();
        return  results;
    }
}
