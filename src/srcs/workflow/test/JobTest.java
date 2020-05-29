package srcs.workflow.test;

import srcs.workflow.job.*;

import java.util.*;

import static org.junit.Assert.*;

public class JobTest {

	private final JobForTest job;
	


	private final Map<String,Object> expected_results;
	
	public JobTest(JobForTest job, Map<String, Object> expected_results) {
		this.job = job;
		this.expected_results = expected_results;
	}
	
	public JobForTest getJob() {
		return job;
	}
	public void check(Map<String, Object> results) throws ValidationException {
		
		//verification des résultats de chaque tache
		assertEquals(expected_results.size(), results.size());
		for(String key : results.keySet()) {
			//System.err.println("Check task "+key+" on job "+job.getName());
			assertEquals(expected_results.get(key), results.get(key));
		}
		
		
		//verification que l'ordre des dépendance dans l'exécution a bien été respectée
		JobValidator jv = new JobValidator(job);
		for(String task_id : jv.getTaskGraph()) {
			for(String pred_id : jv.getTaskGraph().getNeighborsIn(task_id)) {
				assertTrue("task_id = "+task_id+" pred_id = "+pred_id, job.getMappingTaskStartStop().get(task_id).getStart() >= job.getMappingTaskStartStop().get(pred_id).getStop());
			}
		}
	}
			
}
