package srcs.workflow.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import srcs.workflow.graph.Graph;
import srcs.workflow.job.Context;
import srcs.workflow.job.Job;
import srcs.workflow.job.JobValidator;
import srcs.workflow.job.LinkFrom;
import srcs.workflow.job.Task;
import srcs.workflow.job.ValidationException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestJobValidator {


	//test job sans méthodes
	@Test
	public void test1() {
		@SuppressWarnings("serial")
		Job job =new Job("job",new HashMap<String, Object>()) {

		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}


	//test job avec méthode Task statique
	@SuppressWarnings("serial")
	private static class Job2  extends Job{
		public Job2(String name, Map<String, Object> context) {
			super(name, context);
		}

		@Task("A")
		public static Integer f() {return 3;}
	}
	@Test
	public void test2()  {
		Job job =new Job2("Job",new HashMap<>());
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}



	//test job avec task qui retourne void
	@Test
	public void test3()  {
		@SuppressWarnings("serial")
		Job job =new Job("Job",new HashMap<>()) {
			@Task("A")
			public void f() {}
		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}



	//test avec task qui ont le même id
	@Test
	public void test4()  {
		@SuppressWarnings("serial")
		Job job =new Job("Job",new HashMap<>()) {
			@Task("A")
			public Integer f() {return 2;}
			@Task("A")
			public Integer g() {return 3;}
		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}


	//test parametre sans annotation
	@Test
	public void test5() {
		@SuppressWarnings("serial")
		Job job =new Job("Job",new HashMap<>()) {
			@Task("A")
			public  Integer f() {return 2;}
			@Task("B")
			public  Integer g(int x) {return 3;}
		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}


	//test parametre lien vers tache inexistante
	@Test
	public void test6()  {
		@SuppressWarnings("serial")
		Job job =new Job("Job",new HashMap<>()) {
			@Task("A")
			public  Integer f() {return 2;}
			@Task("B")
			public  Integer g(@LinkFrom("C") Integer x) {return 3;}
		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}


	//test fausse correspondance de type entre dépendances
	@Test
	public void test7()  {
		@SuppressWarnings("serial")
		Job job =new Job("Job",new HashMap<>()) {
			@Task("A")
			public Integer f() {return 2;}
			@Task("B")
			public Integer g(@LinkFrom("A") String x) {return 3;}
		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}


	//test cycle dans les dépendances
	@Test
	public void test8()  {
		@SuppressWarnings("serial")
		Job job =new Job("Job",new HashMap<>()) {
			@Task("A")
			public  Integer f(@LinkFrom("B") Integer x) {return 2;}

			@Task("B")
			public  Integer g(@LinkFrom("A") Integer x) {return 3;}
		};
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}


	//test correspondance avec le graphe
	@Test
	public void test9() throws ValidationException, NoSuchMethodException, SecurityException {
		@SuppressWarnings("serial")
		Job job = new Job("Job", new HashMap<>()) {
			@Task("A")
			public Integer a() {return 2;}

			@Task("B")
			public String b() {return "5";}

			@Task("C")
			public String c(@LinkFrom("A")Integer a, @LinkFrom("B")String b) {return "2";}

			@Task("D")
			public Double d(@LinkFrom("C")String a) {return 0.2;}

			@Task("E")
			public Boolean e(@LinkFrom("A")Integer a,@LinkFrom("C")String c) {return true;}
		};

		JobValidator  jv= new JobValidator(job);
		assertEquals(job, jv.getJob());
		assertThrows(IllegalArgumentException.class, () ->jv.getMethod("Z"));
		assertEquals(job.getClass().getMethod("a"), jv.getMethod("A"));
		assertEquals(job.getClass().getMethod("b"), jv.getMethod("B"));
		assertEquals(job.getClass().getMethod("c", Integer.class,String.class), jv.getMethod("C"));
		assertEquals(job.getClass().getMethod("d", String.class), jv.getMethod("D"));
		assertEquals(job.getClass().getMethod("e",Integer.class,String.class), jv.getMethod("E"));

		Graph<String> graph = jv.getTaskGraph();
		assertEquals(5, graph.size());
		assertTrue(graph.existEdge("A", "C"));
		assertTrue(graph.existEdge("A", "E"));
		assertTrue(graph.existEdge("B", "C"));
		assertTrue(graph.existEdge("C", "E"));
		assertTrue(graph.existEdge("C", "D"));

	}




	@SuppressWarnings("serial")
	private static class JobWithContext extends Job{
		public JobWithContext(String name, Map<String, Object> context) {
			super(name, context);
		}

		@Task("A")
		public Integer a() {return 2;}
		
		@Task("B")
		public String b() {return "5";}
		
		@Task("C")
		public String c(@LinkFrom("A")Integer a, @LinkFrom("B")String b, @Context("Z") Integer z) {return "2";}
		
		@Task("D")
		public Double d(@LinkFrom("C")String a) {return 0.2;}

		@Task("E")
		public Boolean e(@LinkFrom("A")Number a,@LinkFrom("C")String c, @Context("X") String x) {return true;}
	}
	
	
	
	//test avec définition partielle de parametre de contexte
	@Test
	public void test10() {
		Map<String,Object> context  =new HashMap<>();
		Job job =new JobWithContext("Job",context);
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}

	
	//test avec non correspondance des types entre le contexte et les parametres
	@Test
	public void test11() {
		Map<String,Object> context  =new HashMap<>();
		context.put("X", 5);
		context.put("Z", 5);
		Job job =new JobWithContext("Job",context);
		assertThrows(ValidationException.class, () ->new JobValidator(job));
	}
	
	
	//test context OK
	@Test
	public void test12() throws ValidationException {
		Map<String,Object> context  =new HashMap<>();
		context.put("X", "Bonjour");
		context.put("Z", Integer.valueOf(5));
		Job job =new JobWithContext("Job",context);
		JobValidator jv = new JobValidator(job);
		assertEquals(job, jv.getJob());
		assertEquals(context,jv.getJob().getContext());
	}
	
	
}
