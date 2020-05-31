package srcs.workflow.server.distributed;

import javafx.util.*;
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


    boolean doneJob;
    Queue<Pair<RemoteJobDistributedExecutor, Job>> jobQueue = new ArrayDeque<>();
    Map<String, Object> resultsMaster;

    Set<TaskTrackerRemote> taskTrackerSet;
    List<CurrentTastTrackerState> trackerStateList;


    static JobTrackerMaster jobTrackerMaster;
    RemoteJobDistributedExecutor executor;
    Lock trakersLock = new ReentrantLock();
    Condition condition = trakersLock.newCondition();


    //HeartBeat

    public static class CurrentTastTrackerState {
        public Job currJob;
        TaskTrackerRemote tt;
        List<String> tasksforTracker;
        long lastHeartBeat;

        public CurrentTastTrackerState(TaskTrackerRemote tt, List<String> tasksforTracker, long lastHeartBeat) {
            this.tt = tt;
            this.tasksforTracker = tasksforTracker;
            this.lastHeartBeat = lastHeartBeat;
        }
    }


    public JobTrackerMaster(Map<String, Object> resultsMaster) {
        doneJob = false;
        this.trackerStateList = new ArrayList<>();
        this.resultsMaster = resultsMaster;
        this.taskTrackerSet = new HashSet<>();
        RemoteJobDistributedExecutor executor;
        (new Thread(() -> {
            while (true) {
                Pair<RemoteJobDistributedExecutor, Job> p;
                synchronized (jobQueue) {
                    while (jobQueue.isEmpty()) {
                        try {
                            System.out.println("No jobs to do");
                            jobQueue.wait();
                            System.out.println("Job notification received");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    p = jobQueue.poll();


                }
                try {
                    ditributeJob(p.getKey(), p.getValue());
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }

        })).start();

    }


    private void ditributeJob(RemoteJobDistributedExecutor executor, Job job) throws RemoteException {
        //pop from queue
        JobValidator jv = null;
        try {
            jv = new JobValidator(job);
        } catch (ValidationException e) {
            e.printStackTrace();
        }

        //se servir des taskTraker comme des threads?
        assert jv != null;
        Graph<String> graph = jv.getTaskGraph();

        List<String> allNode = new ArrayList<>();
        for (String s : graph) {
            allNode.add(s);
        }
        trakersLock.lock();
        try {
            TaskTrackerRemote freetracker;
            while (!allNode.isEmpty() && !resultsMaster.keySet().containsAll(allNode)) {
                while ((freetracker = getFreeTracker()) == null) {
                    System.out.println("No trackers available, sleep");
                    condition.await(3, TimeUnit.SECONDS);
                    System.out.println("Woke up, there is a tracker");

                }
                String taskTosubmit = allNode.get(0);
                allNode.remove(0);
                freetracker.initLocks(graph);
                freetracker.sumbitTask(job, taskTosubmit);

                try {
                    addStateTrackerTask(freetracker, taskTosubmit, job);
                } catch (ConnectException e) {
                    allNode.add(taskTosubmit);
                }

                System.out.println("Submitted the job " + taskTosubmit);
            }
            while (resultsMaster.keySet().size() != graph.size()) {
                condition.await(3, TimeUnit.SECONDS);
            }
            executor.notifyDoneJob(resultsMaster);
            //normalement notification de reception...
            //Thread.sleep(2000);
            resultsMaster = new HashMap<>();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            trakersLock.unlock();
        }


    }

    private void addStateTrackerTask(TaskTrackerRemote sender, String taskTosubmit, Job job) throws RemoteException {
        System.out.println("adding " + sender.getId() + " 's " + taskTosubmit + " to tracker");

        for (CurrentTastTrackerState ttState : trackerStateList) {
            try{


            if (ttState.tt.getId().equals(sender.getId())) {
                ttState.tasksforTracker.add(taskTosubmit);
                ttState.currJob = job;
            }
            }catch (ConnectException e ){
                System.out.println("Not adding tracker");
            }

        }
    }


    public static void main(String[] args) {
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

        jobTrackerMaster = new JobTrackerMaster(new HashMap<>());
        Registry registry = null;
        try {
            registry = LocateRegistry.getRegistry("localhost");
            UnicastRemoteObject.exportObject(jobTrackerMaster, 0);
            registry.rebind("masterRemote", jobTrackerMaster);
            System.out.println("Binded master, sleeping");
            Thread.sleep(800);
            int i = 0;

            while (true) {
                try {
                    TaskTrackerRemote rExecService = (TaskTrackerRemote) registry.lookup("tt" + i++);
                    jobTrackerMaster.taskTrackerSet.add(rExecService);
                    jobTrackerMaster.trackerStateList.add(
                            new CurrentTastTrackerState(rExecService,
                                    new ArrayList<>(), System.currentTimeMillis())
                    );
                    rExecService.registerMaster(jobTrackerMaster);
                    System.out.println("Added tracker " + (i - 1));
                } catch (NotBoundException e) {
                    System.out.println("End of trackers " + i);
                    break;
                }
            }
            jobTrackerMaster.startMonitoringHeartBeat();
        } catch (RemoteException | InterruptedException e) {
            e.printStackTrace();
        }


        try {
            Thread.sleep(80000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    final Object heartHeatLock = new Object();

    private void startMonitoringHeartBeat() {


        (new Thread(() -> {
            //verifyHeartBeat
            while (true) {

                synchronized (heartHeatLock){
                //if there is a malfunction get the Lost set and distribute it
                int i = 0;

                    try {
                        heartHeatLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                    System.out.println("Tt state size "+ trackerStateList.size());
                List<CurrentTastTrackerState > lToRemove = new ArrayList<>();
                for (CurrentTastTrackerState ttState : trackerStateList) {

                    long diff = System.currentTimeMillis() - ttState.lastHeartBeat;
                    System.out.println("Current heart " + (i++) + " beat diff " + TimeUnit.MILLISECONDS.toSeconds(diff));
                    if (TimeUnit.MILLISECONDS.toSeconds(diff) > 7) {
                        taskTrackerSet.remove(ttState.tt);
                        //we lost him
                        List<String> tasksAffected = ttState.tasksforTracker;
                        StringBuilder ka= new StringBuilder();
                        for(String st : tasksAffected){
                            ka.append(",").append(st);
                        }
                        System.out.println("Lost tasks"+ ka);
                        Job job = ttState.currJob;
                        TaskTrackerRemote freetracker;
                        lToRemove.add(ttState);
                        trakersLock.lock();
                        try{


                        while (!tasksAffected.isEmpty() && !resultsMaster.keySet().containsAll(tasksAffected)) {
                            while (true) {
                                try {
                                    if (((freetracker = getFreeTracker()) != null)) break;
                                } catch (RemoteException e) {
                                    e.printStackTrace();
                                }
                                System.out.println("No trackers available, sleep heartbeat");
                                try {
                                    condition.await(3, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                System.out.println("Woke up, there is a tracker in heartbeat");

                            }
                            String taskTosubmit = tasksAffected.get(0);
                            tasksAffected.remove(0);
                            //freetracker.initLocks(graph); //TODO?
                            try {
                                freetracker.sumbitTask(job, taskTosubmit);
                               // addStateTrackerTask(freetracker, taskTosubmit, job);
                                System.out.println("Heartbeat sent to new tracer");
                            } catch (RemoteException e) {
                                e.printStackTrace();
                            }

                        }
                        }finally {
                            trakersLock.unlock();
                        }





                    }


                }
                trackerStateList.removeAll(lToRemove);

            }}
        })).start();


    }

    @Override
    public void postTaskResult(String oTask, Object result, TaskTrackerRemote sender) throws RemoteException {
        trakersLock.lock();
        try {
            System.out.println("Got task result " + oTask + " res : " + result + " " + System.currentTimeMillis());
            resultsMaster.put(oTask, result);
            removeCurrectTaskFromTracker(sender, oTask);


            for (TaskTrackerRemote tt : taskTrackerSet) {
                try {
                    tt.signal(oTask);
                } catch (ConnectException e) {
                    System.out.println("not sending signal to the broken one");
                }

            }
            condition.signalAll();
        } finally {
            trakersLock.unlock();
        }
        //notifyAllTaskTrakers on oTask;

    }

    private void removeCurrectTaskFromTracker(TaskTrackerRemote sender, String oTask) throws RemoteException {

        for (CurrentTastTrackerState ttState : trackerStateList) {
            if (ttState.tt.getId().equals(sender.getId())) {
                ttState.tasksforTracker.remove(oTask);
            }
        }
    }

    @Override
    public boolean requirementsMet(String task) throws RemoteException {
        trakersLock.lock();
        try {
            return resultsMaster.containsKey(task);
        } finally {
            trakersLock.unlock();
        }

    }

    @Override
    public Map<String, Object> resultsRemote() throws RemoteException {

        return resultsMaster;

    }

    @Override
    public void reset() throws RemoteException {
        resultsMaster = new HashMap<>();
    }


    @Override
    public void submitJob(RemoteJobDistributedExecutor executor, Job job) throws RemoteException {
        synchronized (jobQueue) {
            jobQueue.add(new Pair<>(executor, job));
            jobQueue.notify();
        }
    }

    @Override
    public void registerExecutor(RemoteJobDistributedExecutor executor) throws RemoteException {
        this.executor = executor;
    }

    @Override
    public boolean doneJob() throws RemoteException {
        return doneJob;
    }

    @Override
    public void sendHeartBeat(TaskTrackerRemote sender, long lastHB) throws RemoteException {

        for (CurrentTastTrackerState ttState : trackerStateList) {
            if (ttState.tt.getId().equals(sender.getId())) {
                synchronized (heartHeatLock){
                    ttState.lastHeartBeat = lastHB;
                    heartHeatLock.notify();
                }
            }
        }
    }

    private TaskTrackerRemote getFreeTracker() throws RemoteException {
        int i = 0;

            for (TaskTrackerRemote tt : taskTrackerSet) {
                try {
                //System.out.println("cap "+tt.getCapacity()+" > curr "+tt.getCurrentOccupation());
                if (tt.getCapacity() > tt.getCurrentOccupation()) {
                    i++;
                    return tt;
                }
            } catch (ConnectException e) {
                System.out.println("Coonection excetpion on " + i);
            }
            }


        return null;
    }
}
