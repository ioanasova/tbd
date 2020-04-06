package com.example.demo;

import java.io.FileWriter;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Graph {

	Map<String, HashSet<String>> adj;

	Graph() {
		adj = new HashMap<String, HashSet<String>>();
	}

	void addNode(String node) {
		adj.put(node, new HashSet<String>());
	}

	public void addNeighbor(String v1, String v2) {
		adj.get(v1).add(v2);
	}

	public HashSet<String> getNeighbors(String v) {
		return adj.get(v);
	}
	
	public void printGraph() {
		for(Map.Entry<String, HashSet<String>> entry: adj.entrySet()) {
			System.out.println("Node " + entry.getKey() + "--> {");
			for(String item: entry.getValue()) {
				System.out.print(item +", ");
			}
			System.out.println("}");
		}
	}
	
	void writeJson() {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(adj);

        try (FileWriter file = new FileWriter("resources/jsonResult.json")) {
 
            file.write(jsonString);
            file.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}
