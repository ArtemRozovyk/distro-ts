package srcs.workflow.server.central;

import java.rmi.*;

public interface ClientRemote extends Remote {
    void notify(String s) throws RemoteException;
}
