package srcs.workflow.executor;

import srcs.workflow.job.*;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.locks.*;

public class JobExecutorParallel extends JobExecutor {

    Map<String, Lock> lockMap;
    Map<String, Condition> conditionMap;

    public JobExecutorParallel(Job job) {
        super(job);

        lockMap=new HashMap<>();
        conditionMap = new HashMap<>();
        for (String n : graph) {
            Lock lock = new ReentrantLock();
            lockMap.put(n, lock);
            conditionMap.put(n, lock.newCondition());
        }
    }

    @Override
    public Map<String, Object> execute() throws Exception {

        List<Thread> threads = new ArrayList<>();
        for (String s : graph) {
            Thread t = (new Thread(() -> {
                //puts current thread on sleep while there are unmet requirements
                //System.out.println("Trying to exec "+s);
                checkRequirements(s);
                lockMap.get(s).lock();
                try {
                    tryInvokeWithId(s);
                    //System.out.println("executed "+s);
                    conditionMap.get(s).signal();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    lockMap.get(s).unlock();
                }

            }));
            threads.add(t);
            t.start();

        }
        for (Thread t : threads){
            t.join();
        }

        return result;
    }

    private void checkRequirements(String s) {
        Method m = getMethodWithId(s);
        for (Parameter p : m.getParameters()) {
            LinkFrom linkFrom = p.getAnnotation(LinkFrom.class);
            if (linkFrom != null) {
                lockMap.get(linkFrom.value()).lock();
                try {
                    //wait if value hasn't yet been calculated
                    while (!result.containsKey(linkFrom.value())) {
                        //System.out.println("Putting "+s+ " to sleep "+linkFrom.value()+" is not there");
                        conditionMap.get(linkFrom.value()).await();
                        //System.out.println("Waking up "+s);

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lockMap.get(linkFrom.value()).unlock();
                }
            }
        }
    }

}
