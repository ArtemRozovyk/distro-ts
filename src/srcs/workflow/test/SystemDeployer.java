package srcs.workflow.test;

import org.junit.*;

import java.io.*;
import java.lang.reflect.*;

public class SystemDeployer {

    Process processjobtracker;
    Process[] processesTaskTracker = new Process[0];

    String name_class_jobtracker = "srcs.workflow.server.central.JobTrackerCentral";

    String name_class_tasktracker = "";
    int nb_tasktracker = 2;
    int max_slot = 2;

    final String path_project = System.getProperty("user.dir");
    final String path_bin = path_project + "/out/production/SRCS_final"; //FIXME Intellij n'a pas de bin.
    final static String jobtracker = "/usr/lib/jvm/oracle-java8-jdk-amd64/bin/java -javaagent:/home/groku/Documents/ideaIU-2020.1/idea-IU-201.6668.121/lib/idea_rt.jar=39729:/home/groku/Documents/ideaIU-2020.1/idea-IU-201.6668.121/bin -Dfile.encoding=UTF-8 -classpath /usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/charsets.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/deploy.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/cldrdata.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/dnsns.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/jaccess.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/jfxrt.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/localedata.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/nashorn.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/sunec.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/zipfs.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/javaws.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jce.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jfr.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jfxswt.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jsse.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/management-agent.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/plugin.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/resources.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/rt.jar:/home/groku/Desktop/pstl/SRCS_final/out/production/SRCS_final:/home/groku/Desktop/srcs/projetFinal/junit-4.13.jar:/home/groku/.m2/repository/org/hamcrest/hamcrest/2.2/hamcrest-2.2.jar srcs.workflow.server.distributed.JobTrackerMaster";
    final static String tasktt = "/usr/lib/jvm/oracle-java8-jdk-amd64/bin/java -javaagent:/home/groku/Documents/ideaIU-2020.1/idea-IU-201.6668.121/lib/idea_rt.jar=38079:/home/groku/Documents/ideaIU-2020.1/idea-IU-201.6668.121/bin -Dfile.encoding=UTF-8 -classpath /usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/charsets.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/deploy.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/cldrdata.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/dnsns.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/jaccess.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/jfxrt.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/localedata.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/nashorn.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/sunec.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/sunjce_provider.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/sunpkcs11.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/ext/zipfs.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/javaws.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jce.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jfr.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jfxswt.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/jsse.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/management-agent.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/plugin.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/resources.jar:/usr/lib/jvm/oracle-java8-jdk-amd64/jre/lib/rt.jar:/home/groku/Desktop/pstl/SRCS_final/out/production/SRCS_final:/home/groku/Desktop/srcs/projetFinal/junit-4.13.jar:/home/groku/.m2/repository/org/hamcrest/hamcrest/2.2/hamcrest-2.2.jar srcs.workflow.server.distributed.TaskTracker";


    @Before
    public void startAll() throws IOException, InterruptedException {

        processjobtracker = startJVM(jobtracker,
                name_class_jobtracker,
                new String[0],
                System.getProperty("java.io.tmpdir") + "/" + name_class_jobtracker);
        System.out.println("Started jobtracker");



        Thread.sleep(500);
        if (!name_class_tasktracker.equals("")) {
            System.out.println("Starting tasktracker");
            processesTaskTracker = new Process[nb_tasktracker];
            for (int i = 0; i < nb_tasktracker; i++) {
                processesTaskTracker[i] = startJVM(tasktt,
                        name_class_tasktracker,
                        new String[]{"" + i, max_slot + ""},
                        System.getProperty("java.io.tmpdir") + "/" + name_class_tasktracker + "_" + i);
            }

        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stopAll();
            }
        });
        Thread.sleep(500);

    }

    @After
    public void stopAll() {
        processjobtracker.destroyForcibly();
        for (int i = 0; i < processesTaskTracker.length; i++) {
            processesTaskTracker[i].destroyForcibly();
        }
    }

    public static Process startJVM(String path_bin, String main, String[] args, String prefix) throws IOException {

        String tokens[] = new String[2 + args.length];

        for (int i = 0; i < args.length; i++) {
            tokens[i + 2] = args[i];
        }
        if (args.length == 2) {
            path_bin += " " + args[0] + " " + args[1];
        }
        ProcessBuilder pbuilder = new ProcessBuilder(path_bin.split(" "));
        pbuilder.environment().put("CLASSPATH", "out/production/SRCS_final");
        pbuilder.redirectError(new File(prefix + ".stderr"));
        pbuilder.redirectOutput(new File(prefix + ".stdout"));
        return pbuilder.start();

    }


    public static synchronized long getPidOfProcess(Process p) {
        long pid = -1;

        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                pid = f.getLong(p);
                f.setAccessible(false);
            }
        } catch (Exception e) {
            pid = -1;
        }
        return pid;
    }

}
