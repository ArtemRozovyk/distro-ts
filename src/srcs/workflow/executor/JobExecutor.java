package srcs.workflow.executor;

import srcs.workflow.graph.*;
import srcs.workflow.job.*;

import java.lang.reflect.*;
import java.util.*;

public abstract class JobExecutor {
    protected Job job;
    protected Map<String,Object> contextExec;
    protected Map<String,Object> result;
    protected Graph<String> graph;

    public JobExecutor(Job job) {
        this.job = job;
        result=new HashMap<>();
        try {
            graph=(new JobValidator(job)).getTaskGraph();
        } catch (ValidationException e) {
            e.printStackTrace();
        }
        contextExec=job.getContext();
    }
    public abstract Map<String,Object> execute() throws Exception;
    protected void updateReadyQueue(ArrayDeque<String> readyQueue){
        for(String s : graph){
            if(canBeRan(s)&&!result.containsKey(s)&&!readyQueue.contains(s)){
                readyQueue.addLast(s);
            }
        }
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

    protected void tryInvokeWithId(String id) throws Exception{
        if(!canBeRan(id))throw new Exception("cant be ran");
        if(result.containsKey(id))throw new Exception("Has already been ran");
        Method m = getMethodWithId(id);
        Object[]paramObjects= new Object[m.getParameterCount()];
        int i =0;
        for(Parameter p : m.getParameters()){
            Context context = p.getAnnotation(Context.class);
            if(context!=null){
                paramObjects[i++]=contextExec.get(context.value());
                continue;
            }
            LinkFrom linkFromParam = p.getAnnotation(LinkFrom.class);
            if(linkFromParam!=null){
                paramObjects[i++]=result.get(linkFromParam.value());
            }else{
                throw new Exception("Dependency hasnt been executed yet for "+p.getName()+
                        "in method"+m.getName());
            }
        }
        synchronized (result){
            result.put(id,m.invoke(job,paramObjects));
        }
    }

    protected boolean canBeRan(String id){
        Method method = getMethodWithId(id);
        Parameter [] parameters = method.getParameters();
        for (Parameter p : parameters){
            Context context = p.getAnnotation(Context.class);
            if(context!=null){
                if (!contextExec.containsKey(context.value())){
                    throw new IllegalArgumentException("Context doesnt contain "+context.value());//never occurs
                }
            }
            LinkFrom linkFromParam = p.getAnnotation(LinkFrom.class);
            if(linkFromParam!=null&&!result.containsKey(linkFromParam.value())){
                return false;
            }
        }
        return true;

    }
}
