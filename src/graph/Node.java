package graph;

import java.io.Serializable;
import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;

public class Node{
	
	public Graph graph; // the graph to which the node belongs
	
	public int id; // a unique id - running number
	public String label; // for semantic feasibility checks
	public String color; // for color-coding
	
	public ArrayList<Edge> outgoingEdges = new ArrayList<Edge>(); // edges of which this node is the origin
	public ArrayList<Edge> incomingEdges = new ArrayList<Edge>(); // edges of which this node is the destination
	
	public Node(Graph g, int id, String label, String color) {
		this.graph = g;
		this.id = id;
		this.label = label;
		this.color = color;
	}
	
	public Node(Graph g, int id, String label) {
		this(g, id, label, "none");
	}
	
	public Node(Graph g, int id) {
		this(g, id, "none");
	}
	
	
}
