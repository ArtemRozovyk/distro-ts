package srcs.workflow.graph;

import java.io.*;
import java.util.*;
import java.util.function.*;

public class GraphImpl<T> implements Graph<T>, Serializable {

    Set <T> noeuds;
    Map<T,List<T>> linksOut;
    Map<T,List<T>> linksIn;

    public GraphImpl() {
        noeuds=new HashSet<>();
        linksOut=new HashMap<>();
        linksIn=new HashMap<>();
    }

    @Override
    public void addNode(T n) throws IllegalArgumentException {
        if(noeuds.contains(n)){
            throw new IllegalArgumentException();
        }
        noeuds.add(n);
    }

    @Override
    public void addEdge(T from, T to) throws IllegalArgumentException {
        if(!existNode(from)||!existNode(to)){//TODO &&?
            throw new IllegalArgumentException("Pas de from or ");
        }
        if(existEdge(from,to)){
            throw new IllegalArgumentException("Lien existe déjà");
        }

        if(linksOut.containsKey(from)){
            linksOut.get(from).add(to);
        }else {
            List<T> out=new ArrayList<>();
            out.add(to);
            linksOut.put(from,out);
        }
        if(linksIn.containsKey(to)){
            linksIn.get(to).add(from);
        }else {
            List<T> in=new ArrayList<>();
            in.add(from);
            linksIn.put(to,in);
        }
    }

    @Override
    public boolean existEdge(T from, T to) {
        return linksIn.containsKey(to) && linksIn.get(to).contains(from);
    }

    @Override
    public boolean existNode(T n) {
        return noeuds.contains(n);
    }

    @Override
    public boolean isEmpty() {
        return noeuds.isEmpty();
    }

    @Override
    public int size() {
        return noeuds.size();
    }

    @Override
    public List<T> getNeighborsOut(T from) throws IllegalArgumentException {
        if(!existNode(from)) throw new IllegalArgumentException();
        return linksOut.containsKey(from)?linksOut.get(from):new ArrayList<>();
    }

    @Override
    public List<T> getNeighborsIn(T to) throws IllegalArgumentException {
        if(!existNode(to)) throw new IllegalArgumentException();

        return linksIn.containsKey(to)?linksIn.get(to):new ArrayList<>();
    }


    @Override
    public Set<T> accessible(T from) throws IllegalArgumentException {
        if(!existNode(from)) throw new IllegalArgumentException();
        if(!linksOut.containsKey(from))
            return new HashSet<>();
        Set<T> set = new HashSet<>(linksOut.get(from));
        List<T> worklist= new ArrayList<>(set);

        while (!worklist.isEmpty()){
            T noeudFront=worklist.get(0);
            worklist.remove(0);
            Set<T> chlds = new HashSet<>(linksOut.containsKey(noeudFront)?
                    linksOut.get(noeudFront):new HashSet<>());
            for (T chld: chlds){
                if(set.add(chld)){
                    worklist.add(chld);
                }
            }
        }
        return set;
    }

    @Override
    public boolean isDAG() {
         for(T t : this ){
             Set<T> accs= accessible(t);
             if(accs.contains(t)){
                 return false;
             }
        };
         return true;
    }

    @Override
    public Iterator<T> iterator() {
        return noeuds.iterator();
    }


}
