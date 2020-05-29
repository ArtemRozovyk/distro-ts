package srcs.workflow.test;


import srcs.workflow.job.*;

import java.io.*;
import java.util.*;
import java.util.function.*;

public class ArithmeticJob extends JobForTest {
			

	public interface Operator extends IntBinaryOperator,Serializable{}

	private static final long serialVersionUID = 1L;
	private final int min_latency;
	private final int max_latency;
	
	private final Operator op;
	
	
	public ArithmeticJob(String name,
                         Map<String, Object> context,
                         File working_dir,
                         Operator op,
                         int min_latency,
                         int max_latency) {
		super(name, context, working_dir);
		this.op=op;
		this.min_latency =  min_latency;
		this.max_latency=max_latency;
	}
	
	private int perform(String id_task, int a, int b) {
		begin(id_task);
		long time_perform = (new Random(min_latency).nextLong() % (max_latency - min_latency)) + min_latency;
		try {
			Thread.sleep(time_perform);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		int res = op.applyAsInt(a, b);
		end(id_task);
		return res;
	}
	
	
	@Task("A")
	public  Integer t1(@Context("x1") Integer a, @Context("x2") Integer b ) {
		return perform("A", a, b);
	}
	
	@Task("B")
	public  Integer t2(@Context("x3") Integer a, @Context("x4") Integer b ) {
		return perform("B", a, b);
	}
	
	@Task("C")
	public  Integer t3(@Context("x5") Integer a, @Context("x6") Integer b ) {
		return perform("C", a, b);
	}
	
	@Task("D")
	public  Integer t4(@Context("x7") Integer a, @Context("x8") Integer b ) {
		return perform("D", a, b);
	}
	
	
	@Task("E")
	public  Integer t5(@LinkFrom("A") Integer a, @LinkFrom("B") Integer b ) {
		return perform("E", a, b);
	}
	
	@Task("F")
	public  Integer t6(@LinkFrom("C") Integer a, @LinkFrom("D") Integer b ) {
		return perform("F", a, b);
	}
	
	
	@Task("G")
	public  Integer t7(@LinkFrom("E") Integer a, @LinkFrom("F") Integer b ) {
		return perform("G", a, b);
	}
	
		
}
