package srcs.workflow.test;

import java.io.*;
import java.util.*;
import java.util.stream.*;

public final class JobTests {

	public static  int min_latency = 50;
	public static  int max_latency = 200;
	
	private JobTests() {	}
	
	public static JobTest jobtest1() {
	
		String name ="jobtest1";
		
		Map<String,Object> context = new HashMap<>();
		IntStream.range(1, 9).forEach(x-> context.put("x"+x, 1));
		
		JobForTest job = new ArithmeticJob(name,
				context, 
				new File(System.getProperty("java.io.tmpdir")+"/"+name), 
				(a,b)-> a+b  ,  min_latency, max_latency) ;
		
		Map<String, Object> expected_results = new HashMap<>();
		expected_results.put("A", Integer.valueOf(2));
		expected_results.put("B", Integer.valueOf(2));
		expected_results.put("C", Integer.valueOf(2));
		expected_results.put("D", Integer.valueOf(2));
		expected_results.put("E", Integer.valueOf(4));
		expected_results.put("F", Integer.valueOf(4));
		expected_results.put("G", Integer.valueOf(8));
		
		return new JobTest(job, expected_results);
	}
	
	public static JobTest jobtest2() {
		
		String name ="jobtest2";
		
		Map<String,Object> context = new HashMap<>();
		IntStream.range(1, 9).forEach(x-> context.put("x"+x, x));
		
		JobForTest job = new ArithmeticJob(name,
				context, 
				new File(System.getProperty("java.io.tmpdir")+"/"+name), 
				(a,b)-> a*b  , min_latency, max_latency) ;
		
		Map<String, Object> expected_results = new HashMap<>();
		expected_results.put("A", Integer.valueOf(2));
		expected_results.put("B", Integer.valueOf(12));
		expected_results.put("C", Integer.valueOf(30));
		expected_results.put("D", Integer.valueOf(56));
		expected_results.put("E", Integer.valueOf(24));
		expected_results.put("F", Integer.valueOf(1680));
		expected_results.put("G", Integer.valueOf(40320));
		return new JobTest(job, expected_results);
	}
	
	
	public static List<JobTest> jobtests(){
		List<JobTest> res = new ArrayList<>();
		res.add(jobtest1());
		res.add(jobtest2());
		return res;
	}
	
}
