package srcs.workflow.server.distributed;

import java.rmi.*;
import java.util.*;

public interface RemoteJobDistributedExecutor extends Remote {

    void notifyDoneJob(Map<String,Object> res) throws RemoteException;
}
