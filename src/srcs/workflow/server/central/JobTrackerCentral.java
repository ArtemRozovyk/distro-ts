package srcs.workflow.server.central;

import srcs.workflow.executor.*;

import java.io.*;
import java.rmi.*;
import java.rmi.registry.*;
import java.rmi.server.*;

public class JobTrackerCentral {
    static RemoteExecutorService serviceExecutor;
    public static void main(String [] args){
        //deploy exec service
        assert (args.length==0);
        serviceExecutor=new RemoteExecutorService();
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "rmiregistry");
        try {
            processBuilder.environment().put("CLASSPATH",
                    "out/production/SRCS_final");
            processBuilder.start();
            System.out.println("Rmi started!");
            Thread.sleep(1000);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        try {


            UnicastRemoteObject.exportObject(serviceExecutor, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind("remoteExecutor", serviceExecutor);
            System.out.println("RemoteExecutorService has been added under name [ remoteExecutor ]");
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            new ProcessBuilder().command("bash", "-c", "killall -q rmiregistry").start();
            System.out.println("Rmi killed!");

            Thread.sleep(1000);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
