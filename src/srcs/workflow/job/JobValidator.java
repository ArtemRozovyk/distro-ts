package srcs.workflow.job;

import srcs.workflow.graph.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.text.*;
import java.util.*;

public class JobValidator {
    Job job;
    Graph<String> graph;
    Map<String,Method> idMethod;

    public JobValidator(Job job) throws ValidationException {
        this.job=job;
        Class<?> jclazz = job.getClass();
        Map<String,Object> context = job.getContext();
        Method [] jobMethods = jclazz.getMethods();
       idMethod = new HashMap<>();
        for (Method m : jobMethods){
            if(Modifier.isStatic(m.getModifiers())){
                throw new ValidationException();
            }
            Task taskm= m.getAnnotation(Task.class);
            if(taskm!=null){

                if(m.getReturnType().equals(void.class)){
                    throw new ValidationException();
                }
                if(idMethod.containsKey(taskm.value())){
                    throw new ValidationException();
                }
                idMethod.put(taskm.value(),m);
            }
        }
        if(idMethod.isEmpty()){
            throw new ValidationException();
        }
        graph= new GraphImpl<>();
        for(String s : idMethod.keySet()){
            graph.addNode(s);
        }
        for (Map.Entry<String,Method> entry : idMethod.entrySet()){

            Parameter[] params=entry.getValue().getParameters();
            for (Parameter p : params){
                boolean isAnnotated = false;
                for (Annotation a : p.getAnnotations()){
                    if (a.annotationType().equals(LinkFrom.class)){
                        isAnnotated=true;
                        if(!idMethod.containsKey(((LinkFrom)a).value())){
                            throw new ValidationException();
                        }else{
                            try {
                                idMethod.get(((LinkFrom)a).value()).getReturnType().asSubclass(p.getType());
                            }catch (ClassCastException e ){
                                throw new ValidationException();
                            }
                        }

                        graph.addEdge(idMethod.get(((LinkFrom)a).value()).getAnnotation(Task.class).value(),entry.getValue().getAnnotation(Task.class).value()
                                );
                    }
                    if(a.annotationType().equals(Context.class)){
                        isAnnotated=true;
                        if(!context.containsKey(((Context)a).value())){
                            throw new ValidationException();
                        }else{
                            if(!context.get(((Context)a).value()).getClass().equals(p.getType())){
                                throw new ValidationException();
                            }
                        }
                    }
                }
                if(!isAnnotated){
                    throw new ValidationException();
                }
            }
        }



        if(!getTaskGraph().isDAG()){
            throw new ValidationException();
        }
    }



    public Graph<String> getTaskGraph(){
        return graph; //reflex
    }

    public Method getMethod(String id) throws IllegalArgumentException{
        if (!idMethod.containsKey(id))
            throw new IllegalArgumentException();
        return idMethod.get(id);
    }


    public Job getJob(){
        return this.job;
    }

}
