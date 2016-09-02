package matcher;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import graph.Edge;
import graph.Graph;
import graph.Node;

public class VF2Matcher {
	String out1 ="";
	public HashSet<Integer> initn;
	public HashSet<Integer> initm;
	ArrayList<ArrayList<Integer>> cl = new ArrayList<ArrayList<Integer>>();
	ArrayList<Integer> cll = new ArrayList<Integer>();
	
	// finds all subgraph isomorphisms and prints them to the console
	// modelGraph is the big graph
	// patternGraph is the small graph which is searched for in the big one
	public ArrayList<ArrayList<Integer>> find(Graph modelGraph, Graph patternGraph, Long initn, Long initm){
		this.initn = new HashSet<Integer>(1);
		this.initm = new HashSet<Integer>(1);
		this.initn.add(Integer.valueOf(initn.toString()));
		this.initm.add(Integer.valueOf(initm.toString()));
		State state = new State(modelGraph, patternGraph);
		this.matchInternal(state, modelGraph, patternGraph);
		return cl;
		
	}
	
	// internal method for finding subgraphs. called recursively
	private void matchInternal(State s, Graph modelGraph, Graph patternGraph) {

		// abort search if we reached the final level of the search tree 
		if (s.depth == patternGraph.nodes.size()) {
			cll = null;
			ArrayList<Integer> cll = new ArrayList<Integer>();
			for (int i = 0 ; i < s.core_2.length ; i++) {		
					cll.add(s.core_2[i]);
			}
			cl.add(cll);	
		} 
		else
		{	
			//System.out.println(s.getSetContent());
			//s.printMapping();
			// get candidate pairs
			Map<Integer,Integer> candiatePairs = this.getCandidatePairs(s, modelGraph, patternGraph);
			
			// iterate through candidate pairs
			for (Integer n : candiatePairs.keySet()) {
				int m = candiatePairs.get(n);
				
				// check if candidate pair (n,m) is feasible 
				if (checkFeasibility(s,n,m)) {
					
					s.match(n, m); // extend mapping
					matchInternal(s, modelGraph, patternGraph); // recursive call
					s.backtrack(n, m); // remove (n,m) from the mapping
					
				}
			}
			
		}
	}
	
	// determines all candidate pairs to be checked for feasibility
	private Map getCandidatePairs(State s, Graph m, Graph p) {
		if (s.depth==0) 
			//the first time the Tin and Tout sets are not yet initialized.
			return this.pairGenerator(initn,initm);
		Map inmap=this.pairGenerator(s.T1in, s.T2in);
		if (inmap.size()>0) return inmap;
		Map outmap=this.pairGenerator(s.T1out,s.T2out);
		return outmap;
	}
	
	// generates pairs of nodes
	// outputs a map from model nodes to pattern nodes
	private Map pairGenerator(Collection<Integer> modelNodes , Collection<Integer> patternNodes) {
		
		TreeMap<Integer,Integer> map = new TreeMap<Integer,Integer>(); // the map storing candidate pairs
		
		// find the largest among all pattern nodes (the one with the largest ID)!
		// Note: it does not matter how to choose a node here. The only important thing is to have a total order, i.e., to uniquely choose one node. If you do not do this, you might get multiple redundant states having the same pairs of nodes mapped. The only difference will be the order in which these pairs have been included (but the order does not change the result, so these states are all the same!).
		int nextPatternNode = -1;
		for (Integer i : patternNodes)
			nextPatternNode = Math.max(nextPatternNode, i);
		
		// generate pairs of all model graph nodes with the designated pattern graph node
		if (nextPatternNode != -1) {

		for (Integer i : modelNodes)
			map.put(i, nextPatternNode);
		
	}
		
		return map; // return node pairs
	}
	
	// checks whether or not it makes sense to extend the mapping by the pair (n,m)
	// n is a model graph node
	// m is a pattern graph node
	private Boolean checkFeasibility(State s , int n , int m) {
		
		
		
		return checkSemanticFeasibility(s, n, m)&&checkSyntacticFeasibility(s, n, m); // return result
		
			
		
	}
	//checks for semantic feasibility of the pair (n,m)
	private Boolean checkSemanticFeasibility(State s, int n, int m){
		
			try {
				return s.modelGraph.nodes.get(n).label.equals(s.patternGraph.nodes.get(m).label);
			} catch (Exception e) {
				
				return false;
			}
		
		
			
	}
	
	//checks for syntactic feasibility of the pair (n,m)
	private Boolean checkSyntacticFeasibility(State s, int n, int m){
		Boolean passed = true;
		passed = passed && checkRpredAndRsucc(s,n,m); // check Rpred / Rsucc conditions (subgraph isomorphism definition)
		passed = passed && CheckRin(s,n,m);
		passed = passed && CheckRout(s,n,m);
		return passed; // return result	
		}
	
	// checks if extending the mapping by the pair (n,m) would violate the subgraph isomorphism definition
	private Boolean checkRpredAndRsucc(State s , int n , int m) {
		
		Boolean passed = true;
		/*
		// check if the structure of the (partial) model graph is also present in the (partial) pattern graph 
		// if a predecessor of n has been mapped to a node n' before, then n' must be mapped to a predecessor of m 
		Node nTmp = s.modelGraph.nodes.get(n);
		for (Edge e : nTmp.incomingEdges) {
			if (s.core_1[e.source.id] > -1) {
				passed = passed && (s.patternGraph.getAdjacencyMatrix()[s.core_1[e.source.id]][m] == 1);
			}
		}
		// if a successor of n has been mapped to a node n' before, then n' must be mapped to a successor of m
		for (Edge e : nTmp.outgoingEdges) {
			if (s.core_1[e.target.id] > -1) {
				passed = passed && (s.patternGraph.getAdjacencyMatrix()[m][s.core_1[e.target.id]] == 1);
			}
		}
		*/
		// check if the structure of the (partial) pattern graph is also present in the (partial) model graph
		// if a predecessor of m has been mapped to a node m' before, then m' must be mapped to a predecessor of n
		Node mTmp = s.patternGraph.nodes.get(m);
		for (Edge e : mTmp.incomingEdges) {
			if (s.core_2[e.source.id] > -1) {
				passed = passed && (s.modelGraph.getAdjacencyMatrix()[s.core_2[e.source.id]][n] == 1);
			}
		}
		// if a successor of m has been mapped to a node m' before, then m' must be mapped to a successor of n
		for (Edge e : mTmp.outgoingEdges) {
			if (s.core_2[e.target.id] > -1) {
				passed = passed && (s.modelGraph.getAdjacencyMatrix()[n][s.core_2[e.target.id]] == 1);
			}
		}
		
		return passed; // return the result
	}
	
	public Boolean CheckRin(State s, int n, int m){
		HashSet<Integer> T1in=(HashSet<Integer>) s.T1in.clone();
		T1in.removeAll(s.modelGraph.getIngoingVertices(n));
		HashSet<Integer> T2in=(HashSet<Integer>) s.T2in.clone();
		T2in.removeAll(s.patternGraph.getIngoingVertices(m));
		Boolean firstExp= T1in.size()>=T2in.size();
		
		HashSet<Integer> T1in2=(HashSet<Integer>) s.T1in.clone();
		T1in2.removeAll(s.modelGraph.getOutgoingVertices(n));
		HashSet<Integer> T2in2=(HashSet<Integer>) s.T2in.clone();
		T2in2.removeAll(s.patternGraph.getOutgoingVertices(m));
		Boolean secoundExp= T1in2.size()>=T2in2.size();
		return firstExp && secoundExp;
		}
	public Boolean CheckRout(State s, int n, int m){
		HashSet<Integer> T1out=(HashSet<Integer>) s.T1out.clone();
		T1out.retainAll(s.modelGraph.getIngoingVertices(n));
		HashSet<Integer> T2out=(HashSet<Integer>) s.T2out.clone();
		T2out.retainAll(s.patternGraph.getIngoingVertices(m));
		Boolean firstExp= T1out.size()>=T2out.size();
		
		HashSet<Integer> T1out2=(HashSet<Integer>) s.T1in.clone();
		T1out2.retainAll(s.modelGraph.getOutgoingVertices(n));
		HashSet<Integer> T2out2=(HashSet<Integer>) s.T2in.clone();
		T2out2.retainAll(s.patternGraph.getOutgoingVertices(m));
		Boolean secoundExp= T1out2.size()>=T2out2.size();
		return firstExp && secoundExp;
	}
	public Boolean CheckRnew(State s, int n, int m){
		HashSet<Integer> TNilt1=calcNTilt1(s);
		TNilt1.retainAll(s.modelGraph.getIngoingVertices(n));
		HashSet<Integer> TNilt2=calcNTilt2(s);
		TNilt2.retainAll(s.patternGraph.getIngoingVertices(m));
		Boolean firstExp= TNilt1.size()>=TNilt2.size();
		
		HashSet<Integer> NTilt12=calcNTilt1(s);
		NTilt12.retainAll(s.modelGraph.getOutgoingVertices(n));
		HashSet<Integer> NTilt22=calcNTilt2(s);
		NTilt22.retainAll(s.patternGraph.getOutgoingVertices(m));
		Boolean secoundExp= NTilt12.size()>=NTilt22.size();
		return firstExp && secoundExp;		
	}
	
	private HashSet<Integer> calcNTilt1(State s){
		HashSet<Integer> N1= new HashSet(s.modelGraph.nodes);
		HashSet<Integer> M1 = new HashSet();
		for (int node:s.core_1)
		M1.add(node);
		HashSet<Integer> T1 = (HashSet<Integer>) s.T1in.clone();
		T1.retainAll(s.T1out);
		HashSet<Integer> NTilt1=N1;
		NTilt1.removeAll(M1);
		NTilt1.removeAll(T1);
		return NTilt1;
	}
	
	private HashSet<Integer> calcNTilt2(State s){
		HashSet<Integer> N2= new HashSet(s.modelGraph.nodes);
		HashSet<Integer> M2 = new HashSet();
		for (int node:s.core_2)
		M2.add(node);
		HashSet<Integer> T2 = (HashSet<Integer>) s.T2in.clone();
		T2.retainAll(s.T2out);
		HashSet<Integer> NTilt2=N2;
		NTilt2.removeAll(M2);
		NTilt2.removeAll(T2);
		return NTilt2;
	}
	
}
