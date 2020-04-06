package com.example.demo;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TbdLab1Application {
	
	private final static int LIMIT = 10;

	public static void main(String[] args) {
		SpringApplication.run(TbdLab1Application.class, args);
		HtmlParser htmlParser = new HtmlParser();
		Graph graph = new Graph();
		Queue<String> queue = new PriorityQueue<String>();
        queue.add("https://docs.oracle.com/en/servers/management.html");
        
        int i=0;
        while(queue.size() > 0 && i < LIMIT) {
        	try {
        		String link = queue.remove();
				boolean flag = htmlParser.saveFile(link, queue, graph);
				if (flag) {
					i++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
        System.out.println("Printing graph: ");
        graph.printGraph();
        MapReduce mapReduce = new MapReduce();
        Map<String, HashSet<String>> adj = graph.adj;
        try {
        	for (Map.Entry<String,HashSet<String>> entry : adj.entrySet()) {
        		String srcId = entry.getKey();
        		mapReduce.map(srcId, adj);
        	}
        	String target = "http://docs.oracle.com/";
        	LinkedList<String> reffererPages = mapReduce.reduce(target);
        	System.out.println("< "+ target + ": ");
        	for(String reffererPage: reffererPages) {
        		System.out.println(reffererPage + ", ");
        	}
        	System.out.println(" >");
        	graph.writeJson();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
