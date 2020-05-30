package srcs.workflow.executor;

import srcs.workflow.graph.*;
import srcs.workflow.job.*;

import java.lang.reflect.*;
import java.util.*;

public class JobExecutorSequential extends JobExecutor{


    public JobExecutorSequential(Job job) {
        super(job);
    }


    @Override
    public Map<String, Object> execute() throws Exception {
        ArrayDeque<String> readyQueue = new ArrayDeque<>();
        updateReadyQueue(readyQueue);
        while(!readyQueue.isEmpty()){
            String ready =readyQueue.pop();
            tryInvokeWithId(ready);
            updateReadyQueue(readyQueue);
        }
        return result;
    }

}
