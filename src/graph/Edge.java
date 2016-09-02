package graph;

import java.io.Serializable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.*;
public class Edge{
	
	public Graph graph; // the graph to which the edge belongs
	
	public Node source; // the source / origin of the edge
	public Node target; // the target / destination of the edge 
	
	// creates new edge
	public Edge(Graph g, Node source, Node target) {
		this.graph = g;
		this.source = source; // store source
		source.outgoingEdges.add(this); // update edge list at source
		this.target = target; // store target
		target.incomingEdges.add(this); // update edge list at target
	}
	
}
