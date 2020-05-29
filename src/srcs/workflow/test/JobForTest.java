package srcs.workflow.test;

import srcs.workflow.job.*;

import java.io.*;
import java.lang.management.*;
import java.util.*;

public abstract class JobForTest extends Job {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final String SEP="_";
	
	private final File working_dir;
	
	private  Map<String,StartStop> mapping_task_startstop=null;
	private  Map<String,Long> mapping_task_thread=null;
	private  Map<String,Integer> mapping_task_pid=null;
	
	
	public JobForTest(String name, Map<String, Object> context, File working_dir) {
		super(name, context);
		this.working_dir=working_dir;
		
	}
	
	
	public void reset() {
		if(!working_dir.exists()) {
			working_dir.mkdir();
		}else {
			if(!working_dir.isDirectory()) {
				working_dir.delete();
				working_dir.mkdir();
			}
		}
		for(File f : working_dir.listFiles()) {
			f.delete();
		}
		mapping_task_thread=null;
		mapping_task_startstop=null;
		mapping_task_pid=null;
	}
		
	public Map<String,Integer> getMappingTaskPid(){
		if(mapping_task_pid == null) {
			computeStat();
		}
		return mapping_task_pid;
	}
	
	public  Map<String,StartStop> getMappingTaskStartStop(){
		if(mapping_task_startstop == null) {
			computeStat();
		}
		return mapping_task_startstop;
	}
	
	public static class StartStop{
		private final long start;
		private final long stop;
		
		public StartStop(long start, long stop) {
			this.start=start;
			this.stop=stop;
		}

		public long getStart() {
			return start;
		}

		public long getStop() {
			return stop;
		}		
	}
	
	public  Map<String,Long> getMappingTaskThread(){
		if(mapping_task_thread == null) {
			computeStat();
		}
		return mapping_task_thread;
	}
	
	private void computeStat() {
		List<String> ends = new ArrayList<>();
		List<String> begins = new ArrayList<>();
		for(File f : working_dir.listFiles()) {
			String name = f.getName();
			if(name.contains("end")) {
				ends.add(name);
			}else {
				begins.add(name);
			}
		}
			
		mapping_task_startstop=new HashMap<>();
		mapping_task_thread = new HashMap<>();
		mapping_task_pid = new HashMap<>();
		for(String end : ends) {
			String tmp[] =end.split(SEP);
			String id_task = tmp[1];
			String pid = tmp[2];
			String id_thread = tmp[3];
				
			mapping_task_thread.put(id_task, Long.parseLong(id_thread));
			mapping_task_pid.put(id_task, Integer.parseInt(pid));
				
			long stop = Long.parseLong(tmp[4]);
			for(String begin : begins) {
				String tmp2[] =begin.split(SEP);
				if(tmp2[1].equals(id_task) && tmp2[2].equals(pid) && tmp2[3].equals(id_thread)) {
					 long start = Long.parseLong(tmp2[4]);
					 mapping_task_startstop.put(id_task, new StartStop(start, stop));
					 break;
				}
			}				
		}
	}
		
	protected void begin(String id_task) {
		String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		
		
		File f = new File(working_dir, "begin"+SEP+id_task+SEP+pid+SEP+Thread.currentThread().getId()+SEP+System.currentTimeMillis());
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	protected void end(String id_task) {
		String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
		
		
		File f = new File(working_dir, "end"+SEP+id_task+SEP+pid+SEP+Thread.currentThread().getId()+SEP+System.currentTimeMillis());
		try {
			f.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
