package com.project;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.client.Client;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

/**
 * InputParser parses every file given to it as input. Each record is parsed and
 * the values are stored in the form of a Map. Each record is then passed to the
 * analyzer which checks the database for some matches
 * 
 * The SSN is hashed and stored in the Map
 * 
 * @author Nandan
 *
 */
public class InputParser {

	public static final String SSN = "SocialSecurityNumber";
	static Logger admin = Logger.getLogger("admin");

	public void readFromFile(Client client, String fileName)
			throws FileNotFoundException {

		PropertyConfigurator.configure("log4j.properties");
		FileInputStream filePath = new FileInputStream(fileName);
		Scanner reader = new Scanner(filePath);

		String delims = "[;]+";
		String arr[];
		int lineCounter = 1;
		while (reader.hasNextLine()) {

			String line = reader.nextLine();
			HashMap<String, Object> data = new HashMap<String, Object>();
			arr = line.split(delims);
			int length = arr.length - 2;
			String splitter = "[:]";
			while (length != -1) {
				String arr1[] = arr[length].split(splitter);
				data.put(arr1[0].trim(), arr1[1].trim());
				length--;
			}
			if (data.containsKey(SSN)) {
				String hashedSSN = Hashing.sha256()
						.hashString(data.get(SSN).toString(), Charsets.UTF_8)
						.toString();
				data.remove(SSN);
				data.put(SSN, hashedSSN);
			}
			admin.info("Query number " + lineCounter);
			AnalyzeData.checkInputFields(data, client);
			data.clear();
			lineCounter++;
		}
		reader.close();

	}

}