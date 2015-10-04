package com.project;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.HashMap;

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

/**
 * Test cases for ESQuery Class
 * 
 * Testing if the data is inserted,updated and queried in ElasticSearch as
 * expected
 * 
 * @author Nandan
 *
 */

public class ESQueryTest {

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
	}

	@AfterClass
	public static void shutClient() {
		client.admin().indices().delete(new DeleteIndexRequest("patient"))
				.actionGet();
		client.close();
	}

	/**
	 * Tests if the input to the elastic search is stored as expected
	 */
	@Test
	public void testInputToES() throws ElasticsearchException, IOException {

		HashMap<String, Object> hm = new HashMap<String, Object>();
		hm.put("FirstName", "Test");
		hm.put("LastName", "Tester");
		hm.put("Address", "B-2 Street xyz");
		ESQuery.executeInsertQuery(hm, client);
		client.admin().indices().prepareRefresh().execute().actionGet();
		SearchResponse response = client.prepareSearch("patient")
				.setTypes("records")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(QueryBuilders.matchQuery("FirstName", "Test"))
				.setExplain(true).execute().actionGet();
		Assert.assertEquals(response.getHits().getTotalHits(), 2);
		client.close();
	}

	/**
	 * Tests the search query
	 */
	@Test
	public void testSearchInES() {

		client.admin().indices().prepareRefresh().execute().actionGet();
		HashMap<String, Object> data = new HashMap<String, Object>();
		HashMap<String, Object> exclude = new HashMap<String, Object>();
		data.put("FirstName", "Test");
		data.put("LastName", "Tester");
		String result = ESQuery.executeSearchQuery(data, exclude, client);
		Assert.assertNotEquals(result, "InvalidResult");
		data.remove("LastName");
		data.put("LastName", "XYZ");
		result = ESQuery.executeSearchQuery(data, exclude, client);
		Assert.assertEquals("InvalidResult", result);

	}

	/**
	 * Tests the update query
	 */
	@Test
	public void testUpdateInEs() {
		HashMap<String, Object> data = new HashMap<String, Object>();
		HashMap<String, Object> exclude = new HashMap<String, Object>();
		data.put("FirstName", "Test");
		data.put("LastName", "Tester");
		exclude.put("Something", null);
		String result = ESQuery.executeSearchQuery(data, exclude, client);
		data.put("DateOfBirth", "11/1/1991");
		ESQuery.executeUpdateQuery(data, client, result);
		client.admin().indices().prepareRefresh().execute().actionGet();
		String newResult = ESQuery.executeSearchQuery(data, exclude, client);
		Assert.assertEquals(result, newResult);
	}
}
