package com.example.demo;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.Queue;
import java.util.logging.Logger;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Component;

@Component
public class HtmlParser {
	
	private Logger logger = Logger.getLogger(HtmlParser.class.getName());
	private FileUtil fileUtil = new FileUtil();

	public boolean saveFile(String link, Queue<String> queue, Graph graph) throws IOException {

		logger.info("<<< Proccessing url " + link + " >>>");
		if(!link.contains(".html")) {
			return false;
		}
		Document document = Jsoup.connect(link).get();
		boolean noFollow = false;
		
		Elements metaElements = document.getElementsByTag("meta");
		if(!metaElements.isEmpty()) {
			for (Element meta : metaElements) {
				if(meta.hasAttr("name")) {
					if(meta.attr("name").equals("robots")) {
						String content = meta.attr("content");
						String contentValues[] = content.split(",");
						for(String contentValue: contentValues) {
							if (contentValue.equals("noindex")) {
								return false;
							}
							if (contentValue.equals("nofollow")) {
								noFollow = true;
							}
						}
					}
				}
			}
		}
		
		URL url = new URL(link);
		String protocol = url.getProtocol();
		String host = url.getHost();
		String path = url.getPath();
		StringBuilder outputDirectory = new StringBuilder();
		outputDirectory.append("resources/output/").append(protocol).append("/").append(host).append(path.substring(0, path.lastIndexOf("/")));
		String fileName = path.substring(path.lastIndexOf("/") + 1);
		File fileOutput = new File(outputDirectory.toString());
		if(!fileOutput.exists()) {
			fileOutput.mkdirs();
		} else {
			boolean fileExists = fileUtil.checkIfFileExist(fileName);
			if (fileExists) {
				return false;
			}
		}
		graph.addNode(link);
		String outputPath = outputDirectory + "/" + fileName;
		PrintWriter writer = new PrintWriter(outputPath);
		
		Elements titles = document.getElementsByTag("title");
		if(!titles.isEmpty()) {
			for (Element title : titles) {
				writer.println(title.text());
			}
		}
		
		if (!noFollow) {
			Elements linkElements = document.getElementsByTag("a");
			if(!linkElements.isEmpty()) {
				for (Element linkElement : linkElements) {
					int indexOfFragment = 0;
					String element = linkElement.absUrl("href");
					indexOfFragment = element.indexOf("#");
					if(indexOfFragment > 0) {
						String elementWithoutFragment = element.substring(0, indexOfFragment);
						if (!elementWithoutFragment.isEmpty()) {
							System.out.println(elementWithoutFragment);
							queue.add(elementWithoutFragment);
							graph.addNeighbor(link, elementWithoutFragment);
						}
					} else {
						if (!element.isEmpty()) {
							System.out.println(element);
							queue.add(element);
							graph.addNeighbor(link, element);
						}
					}
				}
			}
		}
		
		Elements htmlElements = document.getElementsByTag("body");
		if(!htmlElements.isEmpty()) {
			for (Element htmlElement : htmlElements) {
				writer.println(htmlElement.text());
			}
		}
		
		writer.close();
		return true;
		
	}
}
