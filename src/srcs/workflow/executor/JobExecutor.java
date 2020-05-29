package srcs.workflow.executor;

import srcs.workflow.job.*;

import java.util.*;

public abstract class JobExecutor {
    Job job;

    public JobExecutor(Job job) {
        this.job = job;
    }
    public abstract Map<String,Object> execute() throws Exception;

}
