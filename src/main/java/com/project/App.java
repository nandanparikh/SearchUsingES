package com.project;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

/**
 * 
 * Runs the main class for the best search implementation
 * 
 * Calls function for file generation every month and sends the file to be
 * parsed and queried
 * 
 * @author Nandan
 *
 */
public class App {

	private// All information is stored in admin.log file
	static Logger admin = Logger.getLogger("admin");

	public static void main(String[] args) throws ElasticsearchException,
			IOException {
		PropertyConfigurator.configure("log4j.properties");

		// Initializing node for using Elastic Search
		Node node = nodeBuilder().node();
		Client client = node.client();

		client.admin().cluster()
				.health(new ClusterHealthRequest().waitForGreenStatus())
				.actionGet();

		// Preparing index and storing a dummy record
		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder().startObject()
								.field("FirstName", "Nandan")
								.field("DateOfBirth", "22/10/1994")
								.field("SocialSecurityNumber", "1").endObject())
				.execute().actionGet();
		client.admin().indices().prepareRefresh().execute().actionGet();
		System.out.println("STARTED");

		InputParser ip = new InputParser();

		// temp file stores a random record from each month
		// used for creating new records
		PrintWriter writer = new PrintWriter("temp.txt");
		writer.print("");
		writer.close();

		// data file stores data of each month for reference
		File fout = new File("data.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		// every month new data is written to this file
		String fileName = "monthlyData.txt";
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		// New file for each month is created and data is sent for analyzing
		for (int month = 0; month < 12; month++) {

			/*
			 * client.admin().cluster() .health(new
			 * ClusterHealthRequest().waitForGreenStatus()) .actionGet();
			 */
			client.admin().indices().prepareRefresh().execute().actionGet();

			DataFileGeneration.generateFile(month);

			BufferedReader br = null;
			br = new BufferedReader(new FileReader("monthlyData.txt"));
			String sCurrentLine;
			bw.write("Month : " + (month + 1));
			bw.newLine();
			while ((sCurrentLine = br.readLine()) != null) {
				bw.write(sCurrentLine);
				bw.newLine();
			}
			admin.info("Month Number : " + (month + 1));

			// file sent to be parsed
			ip.readFromFile(client, fileName);
			bw.newLine();
			br.close();
		}
		bw.close();
		client.admin().indices().prepareRefresh().execute().actionGet();

		client.admin().indices().delete(new DeleteIndexRequest("patient"))
				.actionGet();
		client.close();
		System.out.println("SHUTDOWN");
		System.exit(0);
	}
}
