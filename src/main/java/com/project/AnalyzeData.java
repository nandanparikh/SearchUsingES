package com.project;

import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.client.Client;

/**
 * Implements the algorithm for the best match in the database
 * 
 * Algorithm details :
 * 
 * Assumptions: The FirstName and DOB are assumed to remain same for every
 * person. Database may not contain all fields of a particular person.
 * 
 * When a best possible match is found the other fields are automatically
 * updated. The possible reason for the match will be mentioned some of the
 * cases in the log file.
 * 
 * 
 * ALGO:
 * 
 * Input Map is checked if it contains FirstName and DOB. If it contains both
 * then these along with the permutations of the SSN,Address and LastName are
 * sent to be queried.
 * 
 * First, a perfect match for all the 5 is checked. Then combinations are 4 are
 * used ( where FirstName and DOB are always present ) This is done till we do
 * not find a perfect match. Then combinations of 3 are used.
 * 
 * If no match found, then we remove the FirstName ( if present ) and then query
 * with the combinations of other 4 parameters. This is done because the
 * database may not contain FirstName in its record. Here if we find a match
 * then we check that the response does not contain a firstName or else it will
 * not be a correct match
 * 
 * Now we remove DOB and use FirstName along with the other 3 parameters.
 * 
 * Last thing is checked with both DOB and FirstName removed. This will be a
 * rare case when the database does not contain both firstName and DOB
 * 
 * If we find a match in any of the above case, then the record is updated with
 * new values
 * 
 * If no match found, then a new record is inserted in the ES database
 * 
 * @author Nandan
 *
 */

public class AnalyzeData {

	private static final String FIRST_NAME = "FirstName";
	private static final String DOB = "DateOfBirth";
	private static final String LAST_NAME = "LastName";
	private static final String SSN = "SocialSecurityNumber";
	private static final String ADDRESS = "Address";
	private static final String INVALID = "InvalidResult";
	public static Client client;
	static Logger admin = Logger.getLogger("admin");

	public static void checkInputFields(HashMap<String, Object> data,
			Client clientInfo) {

		PropertyConfigurator.configure("log4j.properties");
		client = clientInfo;
		if (data.containsKey(FIRST_NAME) && data.containsKey(DOB)) {
			matchWithFirstNameAndDOB(data);
		} else if (data.containsKey(DOB)) {
			matchWithDOB(data);
		} else if (data.containsKey(FIRST_NAME)) {
			matchWithFirstName(data);
		} else {
			matchWithRestOnly(data);
		}
	}

	/**
	 * FirstName and DOB present
	 * 
	 * Permuted with other fields to find a match
	 * 
	 * @param data
	 */
	public static void matchWithFirstNameAndDOB(HashMap<String, Object> data) {

		HashMap<String, Object> storedData = new HashMap<String, Object>();
		HashMap<String, Object> excludeData = new HashMap<String, Object>();
		String result;
		storedData.put(FIRST_NAME, data.get(FIRST_NAME));
		storedData.put(DOB, data.get(DOB));

		// Checking for perfect match when everything available
		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)
				&& data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				// found match
				admin.info("\nFound a match with all 5 fields : \n"
						+ FIRST_NAME + " " + data.get(FIRST_NAME) + "\n"
						+ LAST_NAME + " " + data.get(LAST_NAME) + "\n" + SSN
						+ " " + data.get(SSN) + "\n" + DOB + " "
						+ data.get(DOB) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}

		}

		// Checking for a match when address of a person changes
		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(LAST_NAME);
			} else {
				// found match
				admin.info("\nFound a match with 4 fields . Address might have changed: \n"
						+ FIRST_NAME
						+ " "
						+ data.get(FIRST_NAME)
						+ "\n"
						+ LAST_NAME
						+ " "
						+ data.get(LAST_NAME)
						+ "\n"
						+ SSN
						+ " "
						+ data.get(SSN)
						+ "\n"
						+ DOB
						+ " "
						+ data.get(DOB));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		// Case where SSN of a person might have changed due to becoming a
		// working independent member
		if (data.containsKey(LAST_NAME) && data.containsKey(ADDRESS)) {
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				// found match
				admin.info("\nFound a match with 4 fields. SSN of the person might have changed: \n"
						+ FIRST_NAME
						+ " "
						+ data.get(FIRST_NAME)
						+ "\n"
						+ LAST_NAME
						+ " "
						+ data.get(LAST_NAME)
						+ "\n"
						+ "\n"
						+ DOB
						+ " "
						+ data.get(DOB)
						+ "\n"
						+ ADDRESS
						+ " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		// Lastname changed possibly. Girl marries and still remains the working
		// member so SSN remains same
		if (data.containsKey(SSN) && data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 4 fields. LastName changed possibly : \n"
						+ FIRST_NAME
						+ " "
						+ data.get(FIRST_NAME)
						+ "\n"
						+ SSN
						+ " "
						+ data.get(SSN)
						+ "\n"
						+ DOB
						+ " "
						+ data.get(DOB)
						+ "\n"
						+ ADDRESS
						+ " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		// LastName and Address changes. Working woman marries and changes
		// address
		if (data.containsKey(SSN)) {
			storedData.put(SSN, data.get(SSN));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
			} else {
				admin.info("\nFound a match with 3 fields. LastName and address changed: \n"
						+ FIRST_NAME
						+ " "
						+ data.get(FIRST_NAME)
						+ "\n"
						+ SSN
						+ " "
						+ data.get(SSN)
						+ "\n"
						+ DOB
						+ " "
						+ data.get(DOB));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		// Address and SSN change where a girl marries and keeps same lastName
		if (data.containsKey(LAST_NAME)) {
			storedData.put(LAST_NAME, data.get(LAST_NAME));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 3 fields. Address SSN may have changed : \n"
						+ FIRST_NAME
						+ " "
						+ data.get(FIRST_NAME)
						+ "\n"
						+ LAST_NAME
						+ " "
						+ data.get(LAST_NAME)
						+ "\n"
						+ DOB
						+ " " + data.get(DOB));

				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		// SSN LastName change. Girl marries but remains at same place
		if (data.containsKey(ADDRESS)) {
			storedData.put(ADDRESS, data.get(ADDRESS));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 3 fields. SSN and lastname change : \n"
						+ FIRST_NAME
						+ " "
						+ data.get(FIRST_NAME)
						+ "\n"
						+ ADDRESS
						+ " "
						+ data.get(ADDRESS)
						+ "\n"
						+ DOB
						+ " "
						+ data.get(DOB));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		// Perfect case of divorce
		result = ESQuery.executeSearchQuery(storedData, excludeData, client);
		if (result.equals(INVALID)) {
			// Do nothing
		} else {
			admin.info("\nFound a match with 2 fields. Address SSN LastName change : \n"
					+ FIRST_NAME
					+ " "
					+ data.get(FIRST_NAME)
					+ "\n"
					+ DOB
					+ " " + data.get(DOB));
			ESQuery.executeUpdateQuery(data, client, result);
			return;
		}
		storedData.clear();
		excludeData.clear();
		matchWithDOB(data);
	}

	/**
	 * FirstName if present, not used while matching.
	 * 
	 * Only possible when record already present has firstName missing
	 * 
	 * All other conditions remain the same as previous function
	 * 
	 * @param data
	 */
	public static void matchWithDOB(HashMap<String, Object> data) {

		HashMap<String, Object> storedData = new HashMap<String, Object>();
		String result;
		HashMap<String, Object> excludeData = new HashMap<String, Object>();
		if (data.containsKey(FIRST_NAME)) {
			excludeData.put(FIRST_NAME, data.get(FIRST_NAME));
		}
		storedData.put(DOB, data.get(DOB));

		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)
				&& data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 4 fields : \n" + LAST_NAME
						+ " " + data.get(LAST_NAME) + "\n" + SSN + " "
						+ data.get(SSN) + "\n" + DOB + " " + data.get(DOB)
						+ "\n" + ADDRESS + " " + data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}

		}
		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + LAST_NAME
						+ " " + data.get(LAST_NAME) + "\n" + SSN + " "
						+ data.get(SSN) + "\n" + DOB + " " + data.get(DOB));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		if (data.containsKey(LAST_NAME) && data.containsKey(ADDRESS)) {
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + LAST_NAME
						+ " " + data.get(LAST_NAME) + "\n" + DOB + " "
						+ data.get(DOB) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(SSN) && data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + SSN + " "
						+ data.get(SSN) + "\n" + DOB + " " + data.get(DOB)
						+ "\n" + ADDRESS + " " + data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		if (data.containsKey(SSN)) {
			storedData.put(SSN, data.get(SSN));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + SSN + " "
						+ data.get(SSN) + "\n" + DOB + " " + data.get(DOB));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		if (data.containsKey(ADDRESS)) {
			storedData.put(ADDRESS, data.get(ADDRESS));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + DOB + " "
						+ data.get(DOB) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		storedData.clear();
		excludeData.clear();
		if (data.containsKey(FIRST_NAME)) {
			matchWithFirstName(data);
		} else
			matchWithRestOnly(data);
	}

	/**
	 * DOB if present, not used while matching.
	 * 
	 * Only possible when record already present has DOB missing
	 * 
	 * All other conditions remain the same as previous function
	 * 
	 * @param data
	 */
	public static void matchWithFirstName(HashMap<String, Object> data) {

		HashMap<String, Object> storedData = new HashMap<String, Object>();
		HashMap<String, Object> excludeData = new HashMap<String, Object>();

		String result;
		storedData.put(FIRST_NAME, data.get(FIRST_NAME));

		if (data.containsKey(DOB))
			excludeData.put(DOB, data.get(DOB));

		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)
				&& data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 4 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + LAST_NAME + " "
						+ data.get(LAST_NAME) + "\n" + SSN + " "
						+ data.get(SSN) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}

		}
		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + LAST_NAME + " "
						+ data.get(LAST_NAME) + "\n" + SSN + " "
						+ data.get(SSN));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		if (data.containsKey(LAST_NAME) && data.containsKey(ADDRESS)) {
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + LAST_NAME + " "
						+ data.get(LAST_NAME) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(SSN) && data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + SSN + " "
						+ data.get(SSN) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(SSN)) {
			storedData.put(SSN, data.get(SSN));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + SSN + " "
						+ data.get(SSN));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(ADDRESS)) {
			storedData.put(ADDRESS, data.get(ADDRESS));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(LAST_NAME)) {
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + FIRST_NAME
						+ " " + data.get(FIRST_NAME) + "\n" + LAST_NAME + " "
						+ data.get(LAST_NAME));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		storedData.clear();
		excludeData.clear();
		matchWithRestOnly(data);
	}

	/**
	 * Does not consider FirstName and DOB
	 * 
	 * Only possible when record has both missing
	 * 
	 * @param data
	 */
	public static void matchWithRestOnly(HashMap<String, Object> data) {
		HashMap<String, Object> storedData = new HashMap<String, Object>();
		String result;
		HashMap<String, Object> excludeData = new HashMap<String, Object>();

		if (data.containsKey(DOB))
			excludeData.put(DOB, data.get(DOB));
		if (data.containsKey(FIRST_NAME))
			excludeData.put(FIRST_NAME, data.get(FIRST_NAME));

		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)
				&& data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 3 fields : \n" + LAST_NAME
						+ " " + data.get(LAST_NAME) + "\n" + SSN + " "
						+ data.get(SSN) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}

		}
		if (data.containsKey(SSN) && data.containsKey(LAST_NAME)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(LAST_NAME, data.get(LAST_NAME));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + LAST_NAME
						+ " " + data.get(LAST_NAME) + "\n" + SSN + " "
						+ data.get(SSN));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}

		if (data.containsKey(LAST_NAME) && data.containsKey(ADDRESS)) {

			storedData.put(LAST_NAME, data.get(LAST_NAME));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);

			if (result.equals(INVALID)) {
				storedData.remove(ADDRESS);
				storedData.remove(LAST_NAME);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + LAST_NAME
						+ " " + data.get(LAST_NAME) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(SSN) && data.containsKey(ADDRESS)) {
			storedData.put(SSN, data.get(SSN));
			storedData.put(ADDRESS, data.get(ADDRESS));

			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
				storedData.remove(ADDRESS);
			} else {
				admin.info("\nFound a match with 2 fields : \n" + SSN + " "
						+ data.get(SSN) + "\n" + ADDRESS + " "
						+ data.get(ADDRESS));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		if (data.containsKey(SSN)) {
			storedData.put(SSN, data.get(SSN));
			result = ESQuery
					.executeSearchQuery(storedData, excludeData, client);
			if (result.equals(INVALID)) {
				storedData.remove(SSN);
			} else {
				admin.info("\nFound a match with 1 fields : \n" + SSN + " "
						+ data.get(SSN));
				ESQuery.executeUpdateQuery(data, client, result);
				return;
			}
		}
		storedData.clear();
		excludeData.clear();
		admin.info("Inserting as new record! Possible duplicates maybe found of this record");
		ESQuery.executeInsertQuery(data, client);
	}
}