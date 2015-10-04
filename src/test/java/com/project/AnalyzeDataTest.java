package com.project;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

/**
 * Test cases for ESQuery Class
 * 
 * Testing if the data is inserted,updated and queried in ElasticSearch as
 * expected
 * 
 * @author Nandan
 *
 */

public class AnalyzeDataTest {

	public static Client client;

	@BeforeClass
	public static void startClient() throws ElasticsearchException, IOException {
		Node node = nodeBuilder().node();
		client = node.client();
		client.admin().cluster()
				.health(new ClusterHealthRequest().waitForGreenStatus())
				.actionGet();
		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder().startObject().field("FirstName", "Test")
								.field("DateOfBirth", "22/10/1991")
								.field("SocialSecurityNumber", "1").endObject())
				.execute().actionGet();
		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder()
								.startObject()
								.field("FirstName", "Nandan")
								.field("DateOfBirth", "22101994")
								.field("SocialSecurityNumber",
										Hashing.sha256()
												.hashString("123-12-1234",
														Charsets.UTF_8)
												.toString())
								.field("Address", "Ahmedabad")
								.field("LastName", "Parikh").endObject())
				.execute().actionGet();

		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder()
								.startObject()
								.field("FirstName", "Hemil")
								.field("DateOfBirth", "3041993")
								.field("SocialSecurityNumber",
										Hashing.sha256()
												.hashString("129-11-1234",
														Charsets.UTF_8)
												.toString())
								.field("Address", "Ahmedabad")
								.field("LastName", "Shah").endObject())
				.execute().actionGet();

		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder()
								.startObject()
								.field("FirstName", "Abhijit")
								.field("DateOfBirth", "9101997")
								.field("SocialSecurityNumber",
										Hashing.sha256()
												.hashString("129-11-1234",
														Charsets.UTF_8)
												.toString())
								.field("Address", "Baroda")
								.field("LastName", "Joshi").endObject())
				.execute().actionGet();

		// Rima married to Abhijit. Same SSN
		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder()
								.startObject()
								.field("FirstName", "Rima")
								.field("DateOfBirth", "1731995")
								.field("SocialSecurityNumber",
										Hashing.sha256()
												.hashString("129-11-1234",
														Charsets.UTF_8)
												.toString())
								.field("Address", "Baroda")
								.field("LastName", "Joshi").endObject())
				.execute().actionGet();

		// A person whose record does not have FirstName
		client.prepareIndex("patient", "records")
				.setSource(
						jsonBuilder()
								.startObject()
								.field("DateOfBirth", "22101981")
								.field("SocialSecurityNumber",
										Hashing.sha256()
												.hashString("321-12-3211",
														Charsets.UTF_8)
												.toString())
								.field("Address", "Baroda").endObject())
				.execute().actionGet();
		client.admin().indices().prepareRefresh().execute().actionGet();

	}

	@AfterClass
	public static void shutClient() {
		// client.admin().indices().prepareDelete("_all").get();
		client.admin().indices().delete(new DeleteIndexRequest("patient"))
				.actionGet();
		client.close();
	}

	/**
	 * Inserting two records into database and then trying to get a perfect
	 * match for the second record
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testProperMatch() throws InterruptedException {
		HashMap<String, Object> hm = new HashMap<String, Object>();

		// inserting a record
		hm.put("FirstName", "Test");
		hm.put("LastName", "Tester");
		hm.put("Address", "B-2 Street xyz");
		hm.put("DateOfBirth", "22/2/1991");
		hm.put("SocialSecurityNumber",
				Hashing.sha256().hashString("123-456-789", Charsets.UTF_8)
						.toString());

		ESQuery.executeInsertQuery(hm, client);

		// inserting another record with just name changed
		hm.replace("FirstName", "Test", "Testing");
		ESQuery.executeInsertQuery(hm, client);
		client.admin().indices().prepareRefresh().execute().actionGet();

		hm.put("Value1", "Last visited doctor on xyz");

		// trying to get a match on record number 2
		AnalyzeData.checkInputFields(hm, client);

		client.admin().indices().prepareRefresh().execute().actionGet();

		// checking if we get an updated field
		SearchResponse response1 = client
				.prepareSearch("patient")
				.setTypes("records")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(
						QueryBuilders.matchQuery("Value1",
								"Last visited doctor on xyz")).setExplain(true)
				.execute().actionGet();

		// Checking the response if it matches with the expected response
		Map<String, Object> result = response1.getHits().getAt(0).sourceAsMap();
		Assert.assertEquals(result.get("FirstName"), "Testing");
		Assert.assertEquals(result.get("SocialSecurityNumber"),
				hm.get("SocialSecurityNumber"));
		hm.clear();
	}

	/**
	 * Checks for some updated records if they give correct output in database
	 */
	@Test
	public void testMatchSomeFields() {
		HashMap<String, Object> hm = new HashMap<String, Object>();

		// THE ADDRESS OF A RECORD CHANGED.
		// CHECKING IF WE GET A MATCH WITH UPDATED RECORD
		hm.put("FirstName", "Nandan");
		hm.put("LastName", "Parikh");
		hm.put("DateOfBirth", "22101994");
		hm.put("Address", "Baroda");
		hm.put("Value1", "Lung cancer");
		hm.put("Value2", "Visited doctor on Sep 11");
		AnalyzeData.checkInputFields(hm, client);
		client.admin().indices().prepareRefresh().execute().actionGet();

		// checking if we get an updated field
		SearchResponse response1 = client.prepareSearch("patient")
				.setTypes("records")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery("FirstName", "Nandan"))
				.setExplain(true).execute().actionGet();

		// Checks if the address was updated
		Map<String, Object> result = response1.getHits().getAt(0).sourceAsMap();
		Assert.assertEquals(response1.getHits().getTotalHits(), 1L);
		Assert.assertEquals(result.get("FirstName"), "Nandan");
		Assert.assertEquals(result.get("Address"), "Baroda");
		hm.clear();

		// CHANGING SSN and LASTNAME OF RIMA
		// CASE OF DIVORCE
		hm.put("FirstName", "Rima");
		hm.put("LastName", "Patel");
		hm.put("DateOfBirth", "1731995");
		hm.put("SocialSecurityNumber",
				Hashing.sha256().hashString("133-11-1233", Charsets.UTF_8)
						.toString());

		AnalyzeData.checkInputFields(hm, client);
		client.admin().indices().prepareRefresh().execute().actionGet();

		// checking if we get an updated field
		SearchResponse response2 = client.prepareSearch("patient")
				.setTypes("records")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery("FirstName", "Rima"))
				.setExplain(true).execute().actionGet();

		// Checks if the SSN and lastname was updated
		Map<String, Object> result2 = response2.getHits().getAt(0)
				.sourceAsMap();
		// Checks if we get only one Rima
		Assert.assertEquals(response2.getHits().getTotalHits(), 1L);
		Assert.assertEquals(result2.get("DateOfBirth"), "1731995");
		Assert.assertEquals(result2.get("SocialSecurityNumber"),
				hm.get("SocialSecurityNumber"));
		hm.clear();

		// Record matching DOB and SSN
		// Record previously did not have FirstName
		hm.put("FirstName", "NewPerson");
		hm.put("LastName", "NewLast");
		hm.put("DateOfBirth", "22101981");
		hm.put("SocialSecurityNumber",
				Hashing.sha256().hashString("321-12-3211", Charsets.UTF_8)
						.toString());

		AnalyzeData.checkInputFields(hm, client);
		client.admin().indices().prepareRefresh().execute().actionGet();

		// checking if we get an updated field
		SearchResponse response3 = client.prepareSearch("patient")
				.setTypes("records")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery("DateOfBirth", "22101981"))
				.setExplain(true).execute().actionGet();

		// Checks if firstname was inserted of the person with given DOB and SSN
		Map<String, Object> result3 = response3.getHits().getAt(0)
				.sourceAsMap();
		// Checks if we get only one record
		Assert.assertEquals(response3.getHits().getTotalHits(), 1L);
		Assert.assertEquals(result3.get("DateOfBirth"), "22101981");
		Assert.assertEquals(result3.get("SocialSecurityNumber"),
				hm.get("SocialSecurityNumber"));
		Assert.assertEquals(result3.get("FirstName"), "NewPerson");
		hm.clear();

		// Person with a single match for name and address but DOB changes
		// Will give no match found from the database and insert new record
		hm.put("FirstName", "Nandan");
		hm.put("DateOfBirth", "23101994");
		hm.put("Address", "Ahmedabad");
		AnalyzeData.checkInputFields(hm, client);
		client.admin().indices().prepareRefresh().execute().actionGet();

		/*
		 * client.admin().cluster() .health(new
		 * ClusterHealthRequest().waitForGreenStatus()) .actionGet();
		 */

		// checking if we get an new inserted field
		SearchResponse response4 = client.prepareSearch("patient")
				.setTypes("records")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery("FirstName", "Nandan"))
				.setExplain(true).execute().actionGet();

		// Checks if we get only two records for Nandan
		Assert.assertEquals(response4.getHits().getTotalHits(), 2L);
		hm.clear();
	}
}
