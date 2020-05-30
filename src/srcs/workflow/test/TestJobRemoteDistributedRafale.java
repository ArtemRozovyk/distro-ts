package srcs.workflow.test;

import org.junit.*;
import srcs.workflow.executor.*;
import srcs.workflow.server.distributed.*;

import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;

public class TestJobRemoteDistributedRafale extends TestJobRemoteDistributed {
			
	@Test
	public void test() throws Exception {
		
		Map<JobTest, Map<String, Object>> results = new ConcurrentHashMap<>();
		
		List<Thread> clients = new ArrayList<>();
		
		for(JobTest jobtest: JobTests.jobtests()) {
			Thread t = new Thread( ()->{
				try {
					JobForTest job = jobtest.getJob();
					job.reset();
					JobExecutor je = new JobExecutorRemoteDistributed(job);
					Map<String,Object> res = je.execute();
					results.put(jobtest, res);
				}catch(Exception e) {
					e.printStackTrace();
				}
			});
			clients.add(t);
		}
		
		for(Thread client : clients) {
			client.start();
		}
		
		for(Thread client : clients) {
			client.join();
		}
		
		for(Entry<JobTest, Map<String, Object>> e : results.entrySet()) {
			e.getKey().check(e.getValue());
		}		
	}
	
}
