package srcs.workflow.job;

import java.util.*;

public abstract class Job {
    public String getName() {
        return name;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    private final String name;
    Map<String,Object> context;
    public Job(String name, Map<String, Object> context) {
        this.name = name;
        this.context=context;
    }
}
