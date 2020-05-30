package srcs.workflow.server.distributed;

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
    Job job;
    MasterRemote masterRemote;
    ExecutorService executorService;

    Map<String, Lock> lockMap;
    Map<String, Condition> conditionMap;


    static TaskTracker taskTracker;
    public static void main(String [] args){
        taskTracker=new TaskTracker(args[0],Integer.parseInt(args[1]));
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("localhost");
            UnicastRemoteObject.exportObject(taskTracker, 0);
            registry.rebind("tt"+args[0], taskTracker);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    public TaskTracker(String id, int nbTasks) {
        this.id = id;
        this.nbTasks = nbTasks;
        executorService= Executors.newFixedThreadPool(nbTasks);
    }


    @Override
    public void registerJob(Job job) throws RemoteException {
        this.job=job;
    }

    @Override
    public void registerMaster(MasterRemote masterRemote) throws RemoteException {
        this.masterRemote=masterRemote;
    }




    protected Method getMethodWithId(String id){
        Method[] jmthds=job.getClass().getDeclaredMethods();
        for(Method m : jmthds){
            Task t = m.getAnnotation(Task.class);
            if(t!=null && t.value().equals(id)){
                return m;
            }
        }
        throw new IllegalArgumentException("no method with id "+id);
    }

    protected Object tryInvokeWithId(String id) throws Exception{
        if(!masterRemote.requirementsMet(id))throw new Exception("cant be ran");
        Method m = getMethodWithId(id);
        Object[]paramObjects= new Object[m.getParameterCount()];
        int i =0;
        for(Parameter p : m.getParameters()){
            Context context = p.getAnnotation(Context.class);
            if(context!=null){
                paramObjects[i++]=job.getContext().get(context.value());
                continue;
            }
            LinkFrom linkFromParam = p.getAnnotation(LinkFrom.class);
            if(linkFromParam!=null){
            while (!masterRemote.resultsRemote().containsKey(linkFromParam.value())) {
                //System.out.println("Putting "+s+ " to sleep "+linkFrom.value()+" is not there");
                conditionMap.get(linkFromParam.value()).await();
                //System.out.println("Waking up "+s);
            }

                paramObjects[i++]=masterRemote.resultsRemote().get(linkFromParam.value());
            }else{
                throw new Exception("no linkFrom parap");
            }
        }
        return m.invoke(job,paramObjects);

    }

    @Override
    public void sumbitTask(String s) throws RemoteException {
        executorService.submit(() -> {
            try {
                masterRemote.postTaskResult(s,tryInvokeWithId(s));
            } catch (Exception e) {
                e.printStackTrace();
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
        try {
            //System.out.println("executed "+s);
            conditionMap.get(s).signal();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockMap.get(s).unlock();
        }
    }
}
