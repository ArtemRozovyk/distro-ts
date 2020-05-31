package srcs.workflow.server.distributed;

import srcs.workflow.graph.*;
import srcs.workflow.job.*;
import srcs.workflow.server.central.*;

import java.io.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class JobTrackerMaster implements MasterRemote {





    Map<String,Object> resultsMaster;
    Set<TaskTrackerRemote> taskTrackerSet;
    static JobTrackerMaster jobTrackerMaster;
    Lock trakersLock = new ReentrantLock();
    Condition condition=trakersLock.newCondition();


    public JobTrackerMaster(Map<String, Object> resultsMaster) {
        this.resultsMaster = resultsMaster;

        this.taskTrackerSet=new HashSet<>();


    }

    public static void main(String[]args){
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "rmiregistry");
        try {
            processBuilder.environment().put("CLASSPATH",
                    "out/production/SRCS_final");
            processBuilder.start();
            Thread.sleep(200);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        jobTrackerMaster=new JobTrackerMaster(new HashMap<>());
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("localhost");
            UnicastRemoteObject.exportObject(jobTrackerMaster, 0);
            registry.rebind("masterRemote", jobTrackerMaster);
            System.out.println("Binded master, sleeping");
            Thread.sleep(800);
            int i=0;

            while(true){
                try {
                    TaskTrackerRemote rExecService = (TaskTrackerRemote)  registry.lookup("tt"+i++);
                    jobTrackerMaster.taskTrackerSet.add(rExecService);
                    rExecService.registerMaster(jobTrackerMaster);
                    System.out.println("Added tracker "+(i-1));
                } catch (NotBoundException e) {
                    System.out.println("End of trackers "+i);
                    break;
                }
            }
        } catch (RemoteException | InterruptedException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(80000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void postTaskResult(String oTask, Object result) throws RemoteException {
        trakersLock.lock();
        try{
            System.out.println("Got task result "+ oTask+" res : "+result+" "+System.currentTimeMillis());
            resultsMaster.put(oTask,result);
            for(TaskTrackerRemote tt: taskTrackerSet){
                tt.signal(oTask);
            }
            condition.signal();
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

            return resultsMaster;

    }

    @Override
    public void reset() throws RemoteException {
        resultsMaster=new HashMap<>();
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
            TaskTrackerRemote freetracker;
            while(!allNode.isEmpty()&&!resultsMaster.keySet().containsAll(allNode)){
                while((freetracker=getFreeTracker())==null){
                    System.out.println("No trackers available, sleep");
                    condition.await(3, TimeUnit.SECONDS);
                    System.out.println("Woke up, there is a tracker");

                }
                String taskTosubmit= allNode.get(0);
                allNode.remove(0);
                freetracker.initLocks(graph);
                freetracker.sumbitTask(job,taskTosubmit);
                System.out.println("Submitted the job "+taskTosubmit);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            trakersLock.unlock();
        }


    }

    private TaskTrackerRemote getFreeTracker() throws RemoteException {
        for(TaskTrackerRemote tt : taskTrackerSet){
            //System.out.println("cap "+tt.getCapacity()+" > curr "+tt.getCurrentOccupation());
            if(tt.getCapacity()>tt.getCurrentOccupation()){
                return tt;
            }
        }
        return null;
    }
}
