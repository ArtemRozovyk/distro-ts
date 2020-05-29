package srcs.workflow.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import srcs.workflow.graph.Graph;
import srcs.workflow.graph.GraphImpl;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestGraph {

	@Test
	public void test1() {
		Graph<Object> g = new GraphImpl<>();
		assertTrue(g.isEmpty());
		assertEquals(0,g.size());
	}
	
	@Test
	public void test2() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		assertTrue(g.existNode("A"));
		assertFalse(g.existNode("B"));
		assertThrows(IllegalArgumentException.class, ()->g.addNode("A"));
	}
	
	@Test
	public void test3() {
		Graph<String> g = new GraphImpl<>();
		Map<String,Integer> map = new HashMap<>();
		g.addNode("A"); map.put("A", 0);
		g.addNode("B"); map.put("B", 0);
		g.addNode("C"); map.put("C", 0);
		g.addNode("D"); map.put("D", 0);
		for(String x : g) {
			assertTrue(map.containsKey(x));
			map.put(x, map.get(x)+1);
			assertEquals(1,map.get(x).intValue());
		}
	}
		
	@Test
	public void test4() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		g.addNode("B");
		g.addNode("C");
		g.addNode("D");
		g.addEdge("A", "B");
		assertTrue(g.existEdge("A", "B"));
		assertFalse(g.existEdge("A", "C"));
		assertFalse(g.existEdge("E", "C"));
		assertFalse(g.existEdge("C", "E"));
	}
	
	@Test
	public void test5() {
		Graph<String> g = new GraphImpl<>();
		assertThrows(IllegalArgumentException.class, ()->g.addEdge("A", "B"));
	}
	
	@Test
	public void test6() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		assertThrows(IllegalArgumentException.class, ()->g.addEdge("A", "B"));
	}
	
	@Test
	public void test7() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		g.addNode("B");
		g.addEdge("A", "B");
		assertThrows(IllegalArgumentException.class, ()->g.addEdge("A", "B"));
	}
	
	@Test
	public void test8() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		g.addNode("B");
		g.addNode("C");
		g.addNode("D");
		g.addEdge("A", "B");
		g.addEdge("A", "C");
		g.addEdge("A", "D");
		g.addEdge("B", "D");
		g.addEdge("C", "D");
		
		
		List<String> noA = g.getNeighborsOut("A");
		assertEquals(3,noA.size());
		assertTrue(noA.contains("B"));
		assertTrue(noA.contains("C"));
		assertTrue(noA.contains("D"));
		List<String> niA = g.getNeighborsIn("A");
		assertEquals(0,niA.size());
		
		List<String> noB = g.getNeighborsOut("B");
		assertEquals(1,noB.size());
		assertTrue(noB.contains("D"));
		List<String> niB = g.getNeighborsIn("B");
		assertEquals(1,niB.size());
		assertTrue(niB.contains("A"));
		
		List<String> noC = g.getNeighborsOut("C");
		assertEquals(1,noC.size());
		assertTrue(noC.contains("D"));
		List<String> niC = g.getNeighborsIn("C");
		assertEquals(1,niC.size());
		assertTrue(niC.contains("A"));
		
		
		assertEquals(0,g.getNeighborsOut("D").size());
		List<String> niD = g.getNeighborsIn("D");
		assertEquals(3,niD.size());
		assertTrue(niD.contains("C"));
		assertTrue(niD.contains("B"));
		assertTrue(niD.contains("A"));
		
		
		assertThrows(IllegalArgumentException.class, ()-> g.getNeighborsOut("Z") );	
		assertThrows(IllegalArgumentException.class, ()-> g.getNeighborsIn("Z") );
	}
	
	@Test
	public void test9() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		g.addNode("B");
		g.addNode("C");
		g.addNode("D");
		g.addNode("E");
		g.addEdge("A", "B");
		g.addEdge("A", "C");
		g.addEdge("B", "D");
		g.addEdge("C", "D");
		g.addEdge("D", "E");
		g.addEdge("E", "C");
		
		assertThrows(IllegalArgumentException.class, ()-> g.accessible("Z") );
		
		Set<String> aA = g.accessible("A");
		assertEquals(4, aA.size());
		assertTrue(aA.contains("B"));
		assertTrue(aA.contains("C"));
		assertTrue(aA.contains("D"));
		assertTrue(aA.contains("E"));
		
		
		Set<String> aB = g.accessible("B");
		assertEquals(3, aB.size());
		assertTrue(aB.contains("C"));
		assertTrue(aB.contains("D"));
		assertTrue(aB.contains("E"));
		
		Set<String> aC = g.accessible("C");
		assertEquals(3, aC.size());
		assertTrue(aC.contains("C"));
		assertTrue(aC.contains("D"));
		assertTrue(aC.contains("E"));
		
		Set<String> aD = g.accessible("D");
		assertEquals(3, aD.size());
		assertTrue(aD.contains("C"));
		assertTrue(aD.contains("D"));
		assertTrue(aD.contains("E"));
		
		Set<String> aE = g.accessible("E");
		assertEquals(3, aE.size());
		assertTrue(aE.contains("C"));
		assertTrue(aE.contains("D"));
		assertTrue(aE.contains("E"));		
	}
	
	@Test
	public void test10() {
		Graph<String> g = new GraphImpl<>();
		g.addNode("A");
		g.addNode("B");
		g.addNode("C");
		g.addNode("D");
		g.addNode("E");
		g.addEdge("A", "B");
		g.addEdge("A", "C");
		g.addEdge("B", "D");
		g.addEdge("C", "D");
		g.addEdge("D", "E");
		
		assertTrue(g.isDAG());
		
		
		g.addEdge("E", "C");
		assertFalse(g.isDAG());
		
		
	}
}
