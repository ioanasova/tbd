package com.example.demo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

public class MapReduce {

	public void map(String srcId, Map<String, HashSet<String>> adjList) throws IOException {
		HashSet <String> nodes = adjList.get(srcId);
		for (String node : nodes) {
			String encodedNode = encode(node);
			String encodedSrcId = encode(srcId);
			StringBuilder filePath = new StringBuilder();
			filePath.append("resources/map/").append(encodedNode).append("_").append(encodedSrcId);
			File file = new File(filePath.toString());
			if (!file.exists()) {
				new FileOutputStream(file).close();
			}
		}
	}

	public LinkedList<String> reduce(String target) {
		LinkedList<String> list = new LinkedList<>();
		String encodedTarget = encode(target);
		String [] files = findFiles(encodedTarget);
		for(String file: files) {
			String encodedReffererPage = file.split("_")[1];
			list.add(decode(encodedReffererPage));
		}
		return list;
	}

	public String[] findFiles(String target) {
		File dir = new File("resources/map/");
		FilenameFilter filter = new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(target);
			}
		};
		return dir.list(filter);
	}

	public String encode(String originalInput) {
		return Base64.getEncoder().encodeToString(originalInput.getBytes()).replace("/", ".");
	}
	
	public String decode(String encodedString) {
		byte[] decodedBytes = Base64.getDecoder().decode(encodedString.replace(".", "/"));
		String decodedString = new String(decodedBytes);
		return decodedString;
	}

}
