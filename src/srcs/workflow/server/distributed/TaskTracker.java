package srcs.workflow.server.distributed;

import srcs.workflow.graph.*;
import srcs.workflow.job.*;
import srcs.workflow.server.central.*;

import java.lang.reflect.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class TaskTracker implements TaskTrackerRemote {
    String id;
    int currTasks;
    int nbTasks;
    MasterRemote masterRemote;
    ExecutorService executorService;

    Map<String, Lock> lockMap;
    Map<String, Condition> conditionMap;


    static TaskTracker taskTracker;

    public static void main(String[] args) {
        taskTracker = new TaskTracker(args[0], Integer.parseInt(args[1]));
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("localhost");
            UnicastRemoteObject.exportObject(taskTracker, 0);
            registry.rebind("tt" + args[0], taskTracker);
            System.out.println("binded as tt" + args[0]);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(80000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }






    }


    public TaskTracker(String id, int nbTasks) {
        this.id = id;
        this.nbTasks = nbTasks;
        executorService = Executors.newFixedThreadPool(nbTasks);
    }


    @Override
    public void registerMaster(MasterRemote masterRemote) throws RemoteException {
        this.masterRemote = masterRemote;
        (new Thread(()-> {
            while (true){
                //send heartBeat ever 4 seconds
                try {
                    masterRemote.sendHeartBeat(this,System.currentTimeMillis());
                    System.out.println("Sent heartbeat "+System.currentTimeMillis());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        })).start();
    }


    protected Method getMethodWithId(Job job, String id) {
        Method[] jmthds = job.getClass().getDeclaredMethods();
        for (Method m : jmthds) {
            Task t = m.getAnnotation(Task.class);
            if (t != null && t.value().equals(id)) {
                return m;
            }
        }
        throw new IllegalArgumentException("no method with id " + id);
    }

    protected Object tryInvokeWithId(Job job, String id) throws Exception {
        //if(!masterRemote.requirementsMet(id))throw new Exception("cant be ran");
        Method m = getMethodWithId(job, id);
        Object[] paramObjects = new Object[m.getParameterCount()];
        int i = 0;
        for (Parameter p : m.getParameters()) {
            Context context = p.getAnnotation(Context.class);
            if (context != null) {
                paramObjects[i++] = job.getContext().get(context.value());
                continue;
            }
            LinkFrom linkFromParam = p.getAnnotation(LinkFrom.class);
            if (linkFromParam != null) {
                System.out.println("Trying to get "+linkFromParam.value()+" lock "+Thread.currentThread().getName());
                lockMap.get(linkFromParam.value()).lock();
                System.out.println("Got "+linkFromParam.value()+" lock "+Thread.currentThread().getName());
                Map<String, Object> mres;
                try {
                    while (!(mres=masterRemote.resultsRemote()).containsKey(
                            linkFromParam.value())) {
                        System.out.print("Current master content: [");
                        for(String s: mres.keySet()){
                            System.out.print(s+",");
                        }
                        System.out.println(" ]");
                        System.out.println(linkFromParam.value()+
                                " is not done, sleeping  "+System.currentTimeMillis());
                        conditionMap.get(linkFromParam.value()).await(2,TimeUnit.SECONDS);
                        System.out.println("Woke up, testing...");
                        //System.out.println("Waking up "+s);
                    }
                } finally {
                    lockMap.get(linkFromParam.value()).unlock();
                }

                paramObjects[i++] = mres.get(linkFromParam.value());
                System.out.println("got param "+(i-1)+" "+paramObjects[i-1]);
            } else {
                throw new Exception("no linkFrom parap");
            }
        }
        System.out.println("Returning to executor "+id);
        return m.invoke(job, paramObjects);

    }

    @Override
    public void sumbitTask(Job job, String s) throws RemoteException {


        Future<?> res=executorService.submit(() -> {
            try {
                currTasks++;
                System.out.println("Sumbitted task "+s+" cur "+currTasks);
                masterRemote.postTaskResult(s, tryInvokeWithId(job, s),this);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                currTasks--;
                System.out.println("Task done "+s+	"  "+System.currentTimeMillis());

            }
        });

    }

    @Override
    public String getId() throws RemoteException {
        return id;
    }

    @Override
    public int getCapacity() throws RemoteException {
        return nbTasks;
    }


    @Override
    public int getCurrentOccupation() throws RemoteException {
        return currTasks;
    }

    @Override
    public void signal(String s) throws RemoteException {
        lockMap.get(s).lock();
        try {
            //System.out.println("executed "+s);
            conditionMap.get(s).signalAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockMap.get(s).unlock();
        }
    }

    @Override
    public void initLocks(Graph<String> graph) throws RemoteException {
        if (lockMap == null) {
            lockMap = new HashMap<>();
            conditionMap = new HashMap<>();
            for (String s : graph) {
                ReentrantLock rl = new ReentrantLock();
                conditionMap.put(s, rl.newCondition());
                lockMap.put(s, rl);
            }
        }
    }
}
