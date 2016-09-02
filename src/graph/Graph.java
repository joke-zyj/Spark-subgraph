package graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;

import scala.annotation.serializable;

import com.esotericsoftware.kryo.Kryo;

// class representing a graph
// construct graphs only with the methods provided in this class
public class Graph{
	
	public String name; // name of the graph
	public ArrayList<Node> nodes = new ArrayList<Node>(); // list of all nodes
	public ArrayList<Edge> edges = new ArrayList<Edge>(); // list of all edges
	
	private int[][] adjacencyMatrix; // stores graph structure as adjacecy matrix
	private boolean adjacencyMatrixUpdateNeeded = true; // indicates if the adjacency matrix needs an update
	private int nodeCounter = 0; // counts nodes for assigning id's
	
	public Graph(String name) {
		this.name = name;
	}
	
	// add nodes
	public void addNode(String label, String color) {
		nodes.add(new Node(this, nodeCounter, label, color));
		nodeCounter++;
		this.adjacencyMatrixUpdateNeeded = true;
	}
	
	public void addNode(String label) {
		nodes.add(new Node(this, nodeCounter, label));
		nodeCounter++;
		this.adjacencyMatrixUpdateNeeded = true;
	}
	
	public void addNode() {
		nodes.add(new Node(this, nodeCounter));
		nodeCounter++;
		this.adjacencyMatrixUpdateNeeded = true;
	}
	public void addNode(String label, int nodeCounter, String suibian) {
		nodes.add(new Node(this, nodeCounter, label));
		this.adjacencyMatrixUpdateNeeded = true;
	}
	
	// add edges
	public void addEdge(Node source, Node target) {
		edges.add(new Edge(this, source, target));
		this.adjacencyMatrixUpdateNeeded = true;
	}
	
	public void addEdge(int sourceId, int targetId) {
		this.addEdge(this.nodes.get(sourceId), this.nodes.get(targetId));
	}
	public HashSet<Integer> getIngoingVertices(int targetId){
		int[][] matrix=getAdjacencyMatrix();
		HashSet<Integer> result=new HashSet<Integer>();
		for (int i=0; i< matrix.length;i++){
			if (matrix[i][targetId]==1) result.add(i);
		}
		return result;
	}
	public HashSet<Integer> getOutgoingVertices(int sourceId){
		int[][] matrix=getAdjacencyMatrix();
		HashSet<Integer> result=new HashSet<Integer>();
		for (int i=0; i<matrix.length;i++){
			if (matrix[sourceId][i]==1) result.add(i);
		}
		return result;
	}
	
	// get the adjacency matrix
	// reconstruct it if it needs an update
	public int[][] getAdjacencyMatrix() {
		
		if (this.adjacencyMatrixUpdateNeeded) {
			
			int k = this.nodes.size();
			this.adjacencyMatrix = new int[k][k];
			for (int i = 0 ; i < k ; i++)
				for (int j = 0 ; j < k ; j++)
					this.adjacencyMatrix[i][j] = 0; // initialize entries to 0
			
			for (Edge e : this.edges) {
				this.adjacencyMatrix[e.source.id][e.target.id] = 1; // change entries to 1 if there is an edge
			}
			this.adjacencyMatrixUpdateNeeded = false;
		}
		return this.adjacencyMatrix;
	}
	
	
	// prints adjacency matrix to console
	public String printGraph() {
		/*int[][] a = this.getAdjacencyMatrix();
		int k = a.length;
		
		System.out.print(this.name + " - Nodes: ");
		for (Node n : nodes) System.out.print(n.id + " ");
		System.out.println();
		for (int i = 0 ; i < k ; i++) {
			for (int j = 0 ; j < k ; j++) {
				System.out.print(a[i][j] + " ");
				this.name=this.name+a[i][j]+" ";
			}
			System.out.println();
			
		}*/return this.name;
	}
	
}
