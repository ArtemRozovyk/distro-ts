package srcs.workflow.test;

import org.junit.*;
import srcs.workflow.executor.*;

import java.lang.management.*;
import java.util.*;

import static org.junit.Assert.*;

public class TestJobLocalParallel {

	@Test
	public void test() throws Exception {
		
		for(JobTest jobtest : JobTests.jobtests()) {
			JobForTest job = jobtest.getJob();
			job.reset();
			JobExecutor je = new JobExecutorParallel(job);
			Map<String,Object> res = je.execute();
			jobtest.check(res);
	
			Integer my_pid = Integer.parseInt(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
			assertEquals(job.getMappingTaskThread().size(),job.getMappingTaskThread().values().stream().distinct().count());
			assertEquals(1,job.getMappingTaskPid().values().stream().distinct().count());
			assertEquals(my_pid, job.getMappingTaskPid().get("A"));
		}
		
		
		
	}

}
