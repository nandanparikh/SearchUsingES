package com.project;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.json.JSONObject;

/**
 * Builds and executes different queries for Inserting Deleting and Updating as
 * per input
 * 
 * @author Nandan
 *
 */
public class ESQuery {
	private static final String INVALID = "InvalidResult";
	private static final String INDEX = "patient";
	private static final String TYPE = "records";
	private static Client client;

	/**
	 * Builds a query dynamically using the inputs from the HashMap
	 * 
	 * @param inputData
	 *            HashMap of values to be searched
	 * @return BoolQueryBuilder
	 */
	private static BoolQueryBuilder searchQueryGenerator(
			HashMap<String, Object> inputData,
			HashMap<String, Object> excludeData) {

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();

		for (Map.Entry<String, Object> entry : inputData.entrySet()) {
			boolQuery.must(QueryBuilders.matchQuery(entry.getKey(),
					entry.getValue()));
		}

		return boolQuery;
	}

	/**
	 * Builds a JSONObject of the update query dynamically using the inputs from
	 * the HashMap
	 * 
	 * @param inputData
	 *            HashMap of values to be updated
	 * @return JSONObject
	 * 
	 */
	private static JSONObject updateQueryGenerator(
			HashMap<String, Object> inputData) {

		JSONObject jsonObj = new JSONObject();
		// Maps.
		for (Map.Entry<String, Object> entry : inputData.entrySet()) {
			jsonObj.put(entry.getKey(), entry.getValue());
		}
		return jsonObj;
	}

	/**
	 * Builds a JSONObject of the insert query dynamically using the inputs from
	 * the HashMap
	 * 
	 * @param inputData
	 *            HashMap of values to be inserted
	 * @return JSONObject
	 */
	private static JSONObject insertQueryGenerator(
			HashMap<String, Object> inputData) {

		JSONObject jsonObj = new JSONObject();
		for (Map.Entry<String, Object> entry : inputData.entrySet()) {
			jsonObj.put(entry.getKey(), entry.getValue());
		}
		return jsonObj;
	}

	/**
	 * Searches the ES server with the query after building it and returns the
	 * ID of the record found
	 * 
	 * Checks if more than one responses are present. If yes then the
	 * excludeData is compared. excludeData is the Map of values which must not
	 * be present in the record, otherwise it is not a perfect match as per the
	 * algorithm mentioned in Analyze Data
	 * 
	 * Returns InvalidOutput if none found
	 * 
	 * @param inputData
	 * @param excludeData
	 * @param clientInfo
	 * @return
	 */
	public static String executeSearchQuery(HashMap<String, Object> inputData,
			HashMap<String, Object> excludeData, Client clientInfo) {
		client = clientInfo;
		BoolQueryBuilder boolQuery = searchQueryGenerator(inputData,
				excludeData);
		SearchResponse response = client.prepareSearch(INDEX).setTypes(TYPE)
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				.setQuery(boolQuery).setExplain(true).execute().actionGet();
		int totalResponses = (int) response.getHits().getTotalHits();

		// Checks for multiple responses and tries to get a perfect match
		String result = INVALID;
		int totalValidResults = 0;
		while (totalResponses != 0) {
			Map<String, Object> responseMap = response.getHits()
					.getAt(totalResponses - 1).sourceAsMap();
			int checker = 1;
			for (Map.Entry<String, Object> entry : excludeData.entrySet()) {
				if (responseMap.containsKey(entry.getKey())) {
					checker = 0;
				}
			}
			if (checker == 1) {
				totalValidResults++;
				result = response.getHits().getAt(totalResponses - 1).getId();
			}
			totalResponses--;
		}
		if (totalValidResults == 1)
			return result;
		else
			return INVALID;
	}

	/**
	 * Updates information of the person with the values from HashMap
	 * 
	 * @param inputData
	 * @param clientInfo
	 * @param id
	 */
	public static void executeUpdateQuery(HashMap<String, Object> inputData,
			Client clientInfo, String id) {
		client = clientInfo;
		JSONObject jsonObj = updateQueryGenerator(inputData);
		UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
				.doc(jsonObj.toString());
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		bulkRequest.add(updateRequest);
		bulkRequest.execute().actionGet();
	}

	/**
	 * Inserts the data into ES with the values from HashMap
	 * 
	 * @param inputData
	 * @param clientInfo
	 * @return
	 */
	public static int executeInsertQuery(HashMap<String, Object> inputData,
			Client clientInfo) {
		client = clientInfo;
		JSONObject jsonObj = insertQueryGenerator(inputData);
		IndexRequest indexRequest = new IndexRequest(INDEX, TYPE);
		indexRequest.source(jsonObj.toString());
		// IndexResponse response =
		client.index(indexRequest).actionGet();

		return 0;
	}

}
