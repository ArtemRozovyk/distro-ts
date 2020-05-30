package srcs.workflow.server.distributed;

import srcs.workflow.graph.*;
import srcs.workflow.job.*;
import srcs.workflow.server.central.*;

import java.io.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;
import java.util.concurrent.locks.*;

public class JobTrackerMaster implements MasterRemote {





    Map<String,Object> resultsMaster;
    Set<TaskTracker> taskTrackerSet;
    static JobTrackerMaster jobTrackerMaster;
    Lock trakersLock = new ReentrantLock();
    Condition condition=trakersLock.newCondition();


    public JobTrackerMaster(Map<String, Object> resultsMaster) {
        this.resultsMaster = resultsMaster;

        this.taskTrackerSet=new HashSet<>();
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("localhost");
            int i=0;
            while(true){
                try {
                    TaskTracker rExecService = (TaskTracker)  registry.lookup("tt"+i++);
                    taskTrackerSet.add(rExecService);
                } catch (NotBoundException e) {
                    break;
                }
            }
        } catch (RemoteException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[]args){
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "rmiregistry");
        try {
            processBuilder.environment().put("CLASSPATH",
                    "out/production/SRCS_final");
            processBuilder.start();
            Thread.sleep(3500);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        jobTrackerMaster=new JobTrackerMaster(new HashMap<>());
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("localhost");
            UnicastRemoteObject.exportObject(jobTrackerMaster, 0);
            registry.rebind("masterRemote", jobTrackerMaster);
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void postTaskResult(String oTask, Object result) throws RemoteException {
        trakersLock.lock();
        try{
            resultsMaster.put(oTask,result);
            for(TaskTracker tt: taskTrackerSet){
                tt.signal(oTask);
                condition.signal();
            }
        }finally {
            trakersLock.unlock();
        }
        //notifyAllTaskTrakers on oTask;

    }

    @Override
    public boolean requirementsMet(String task) throws RemoteException {
        trakersLock.lock();
        try{
            return resultsMaster.containsKey(task);
        }finally {
            trakersLock.unlock();
        }

    }

    @Override
    public Map<String, Object> resultsRemote() throws RemoteException {
        trakersLock.lock();
        try{
            return resultsMaster;
        }finally {
            trakersLock.unlock();
        }
    }



    @Override
    public void submitJob(Job job)throws RemoteException {
        JobValidator jv = null;
        try {
             jv = new JobValidator(job);
        } catch (ValidationException e) {
            e.printStackTrace();
        }

        //se servir des taskTraker comme des threads?
        assert jv != null;
        Graph<String> graph=jv.getTaskGraph();
        List<String> allNode = new ArrayList<>();
        for (String s : graph){
            allNode.add(s);
        }
        trakersLock.lock();
        try
        {
            TaskTracker freetracker;
            while(!allNode.isEmpty()&&!resultsMaster.keySet().containsAll(allNode)){
                while((freetracker=getFreeTracker())==null){
                    condition.await();
                }
                String taskTosubmit= allNode.get(0);
                allNode.remove(0);
                freetracker.sumbitTask(taskTosubmit);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            trakersLock.unlock();
        }


    }

    private TaskTracker getFreeTracker() throws RemoteException {
        for(TaskTracker tt : taskTrackerSet){
            if(tt.getCapacity()>tt.getCurrentOccupation()){
                return tt;
            }
        }
        return null;
    }
}
