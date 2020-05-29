package srcs.workflow.graph;

import java.util.*;

public interface Graph<T> extends Iterable<T>{
    void addNode(T n) throws IllegalArgumentException;
    void addEdge(T from, T to) throws IllegalArgumentException;
    boolean existEdge(T from, T to);
    boolean existNode(T n);
    boolean isEmpty();
    int size();
    List<T> getNeighborsOut(T from) throws  IllegalArgumentException;
    List<T> getNeighborsIn(T to) throws IllegalArgumentException;
    Set<T> accessible(T from) throws IllegalArgumentException;
    boolean isDAG();




}
