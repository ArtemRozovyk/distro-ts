package srcs.workflow.job;


import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
public @interface Task {
    String value();
}
