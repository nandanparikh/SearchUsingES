package com.project;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Random;

/**
 * 
 * Generates sample data files
 * 
 * Random function makes the data generated to be different on every run
 * 
 * Also one record from each month is stored in a temp file, so that there is a
 * possibility of collisions
 * 
 * @author Nandan
 *
 */
public class DataFileGeneration {
	
	public static final String FIRST_NAME = "FirstName";
	public static final String LAST_NAME = "LastName";
	public static final String SOCIAL_SECURITY_NUMBER = "SocialSecurityNumber";
	public static final String ADDRESS = "Address";
	public static final String DOB = "DateOfBirth";
	
		static final String[] FIRST_NAMES = new String [] {
			"Suzanne",
			"John",
			"David",
			"Ashley",
			"Leah",
			"Heather",
			"Emily",
			"Isabella",
			"Emma",
			"Ava",
			"Madison",
			"Sophia",
			"Olivia",
			"Abigail",
			"Hannah",
			"Elizabeth",

			"Jacob",
			"Michael",
			"Ethan",
			"Joshua",
			"Daniel",
			"Christopher",
			"Anthony",
			"William",
			"Matthew",
			"Andrew",
			"Ryan",
			"Kevin",
			"Dave",
			"James"
		};
		
		static final String[] LAST_NAMES = new String[] {
			"Chin",
			"Smith",
			"Kim",
			"Jackson",
			"Roberts",
			"Achebe",
			"Baker",
			"Esteban",
			"MacDonald",
			"Lucas",
			"Hernandez",
			"Ramirez",
			"El-Baz",
			"Wilson",
			"Crichton",
			"Philips",
			"Carter",
			"Williams",
			"Nolan",
			"Clarke",
			"Johnson"

		};
		static final String[] STREET_ADDRESS = new String[] {
				"1422 Fagar Center",
				"1548 Fojbi Ridge",
				"1506 Ubko Boulevard",
				"565 Uzeub Extension",
				"980 Paze Heights",
				"993 Welide Place",
				"1967 Zudu Court",
				"1906 Cutdo Glen",
				"918 Ivna Extension",
				"1038 Bamit Point",
				"1646 Kehu Drive",
				"1182 Icujec Key",
				"400 Ujza Junction",
				"995 Sabih Ridge",
				"531 Lesbif Extension",
				"1282 Muphi Street",
				"1431 Cijata Avenue",
				"1300 Zasi Pass",
				"181 Ubcor Heights",
				"826 Gahod Path",
				"490 Belri Mill",
				"1775 Utewem Road",
				"1716 Tuje Terrace",
				"1908 Uhumo Street",
				"1852 Omvi Extension",
				"1517 Ibrem Boulevard",
				"1338 Kevbe Key",
				"1315 Koat Boulevard",
				"1147 Ludil Loop",
				"1647 Feuh Extension",
				"184 Jogoc Lane",
				"1342 Reponu Manor",
				"1070 Iwelo Grove",
				"1634 Ajuju Pass",
				"911 Faet Square",
				"1685 Raha Grove",
				"1724 Erure View",
				"690 Pauz Plaza",
				"493 Meju Place",
				"1590 Jozat Center",
				"638 Sogpiv Turnpike",
				"151 Ashap Grove",
				"314 Bafha Highway",
				"1932 Tuwij Grove",
				"67 Tulwep Court",
				"882 Liduw Mill",
				"772 Nevop Park",
				"601 Onoej River"
		};		
		
		static final String[] CITY = new String[] {
				"New York",
				"Washington",
				"Salem",
				"Chicago",
				"San Francisco",
				"Springfield",
				"Auburn",
				"Greenville",
				"Philadelphia",
				"Houston",
				"Nahville",
				"Detroit",
				"Las Vegas",
				"Portland",
				"Jackson",
				"Fresno",
				"Orlando"
		};
		static final String[] SSN = new String[] {
				"221-38-4797",
				"009-60-6760",
				"400-61-6597",
				"400-61-6597",
				"517-22-5865",
				"568-17-2927",
				"235-88-5147",
				"486-15-4477",
				"665-01-0461",
				"203-44-6190",
				"226-79-1302",
				"502-32-0024",
				"612-14-4684",
				"376-30-7273",
				"517-24-1674",
				"038-36-6988",
				"527-90-8033",
				"045-54-2518",
				"002-76-5670",
				"192-70-2172",
				"625-49-2124",
				"489-23-2468",
				"578-48-7819",
				"673-10-4947",
				"694-05-3315",
				"516-50-0026",
				"405-28-1256",
				"047-16-3873",
				"234-09-0284",
				"517-31-6774",
				"517-31-6774",
				"527-52-0609",
				"647-88-3973",
				"527-98-3487",
				"031-68-2460",
				"122-66-9312",
				"504-32-3393",
				"412-46-1965",
				"524-14-5210"	
		};
		static final String[] DISEASES = new String[] {
			"heart failure",
			"chemical burn",
			"lung cancer",
			"breast cancer",
			"other cancer",
			"repetitive stress",
			"insomnia",
			"hearing loss",
			"arthritis",
			"glaucoma",
			"broken limb",
			"sickle-cell anemia",
			"kidney failure"
		};
		
		static final String[] MEDICINES = new String[] {
			"Borolin",
			"Glucose",
			"Accupril",
			"Accutane",
			"Baclofen",
			"Aspirin",
			"Betagan",
			"Bicillin",
			"Penicillin",
			"Ativan",
			"Codeine",
			"Lexapro",
			"Tramadol",
			"Lyrica",
			"Naproxen",
			"Ibuprofen",
			"Lorazepam",
			"Zoloft",
			"Gabapentin",
			"Doxycycline",
			"Losartan",
			"Xanax",
			"Oxycodone"
		};
		static final String[] TREATMENT = new String[] {
			"Blood Transfusion",
			"Bone Marrrow Replacement",
			"Corneal Transplant",
			"Enzyme Replacement",
			"Estrigen Replacement Therapy",
			"Exercise",
			"Fluid Replacement",
			"Heart Surgery",
			"Heart Transplant",
			"Heat Therapy",
			"Kidney Dialysis",
			"Kidney Transplant",
			"Liver Transplant",
			"Mechanical Ventilation",
			"Phototherapy",
			"Psychotherapy",
			"Radio Surgery",
			"Radiation Therapy",
			"Surgical Drainage",
			"Tumor Surgery"
			
		};

	public static String generateDate() {
		GregorianCalendar gc = new GregorianCalendar();

		int year = randBetween(1900, 2010);

		gc.set(Calendar.YEAR, year);

		int dayOfYear = randBetween(1,
				gc.getActualMaximum(Calendar.DAY_OF_YEAR));

		gc.set(Calendar.DAY_OF_YEAR, dayOfYear);

		String date = gc.get(Calendar.YEAR) + "" + gc.get(Calendar.MONTH) + ""
				+ gc.get(Calendar.DAY_OF_MONTH);

		return date;
	}

	public static int randBetween(int start, int end) {
		return start + (int) Math.round(Math.random() * (end - start));
	}

	public static int tempVar3 = 0;

	public static void generateFile(int number) throws IOException {
		File fout = new File("monthlyData.txt");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		Random t1 = new Random();

		int duplicateEntry = t1.nextInt(10);
		int sameEntryNo = t1.nextInt(10);
		while (sameEntryNo == duplicateEntry) {
			sameEntryNo = t1.nextInt(10);
		}
		for (int i = 0; i < 10; i++) {
			if (i == duplicateEntry && number > 0) {
				BufferedReader br = null;
				br = new BufferedReader(new FileReader("temp.txt"));
				String sCurrentLine;
				int availableEntries = 0;
				while ((sCurrentLine = br.readLine()) != null) {
					availableEntries++;
				}

				int entryNo = t1.nextInt(availableEntries);
				br.close();
				br = new BufferedReader(new FileReader("temp.txt"));
				int tempVar = 0;
				while (tempVar != entryNo) {
					tempVar++;
					br.readLine();
				}
				sCurrentLine = br.readLine();
				String tempString2 = "";
				if (number > 1 && number < 6) {
					String[] tempString = sCurrentLine.split("; ");
					int tempVar4 = 0;
					for (int j = 0; j < tempString.length; j++) {
						int tempVar2 = t1.nextInt(2);

						if (tempVar2 > 0 || tempVar4 > 3) {
							tempString2 = tempString2 + tempString[j] + "; ";
						} else {
							tempVar4++;
						}
					}
					sCurrentLine = tempString2;

				} else if (number > 5) {
					String[] tempString = sCurrentLine.split("; ");

					for (int j = 0; j < tempString.length; j++) {
						if (tempString[j].split(":")[0].equals(FIRST_NAME)) {
							tempVar = t1.nextInt(2);
							tempString2 += tempString[j] + "; ";

						}
						if (tempString[j].split(":")[0].equals(LAST_NAME)) {
							tempVar = t1.nextInt(3);
							if (tempVar == 1) {
								tempString2 += tempString[j] + "; ";
							} else if (tempVar == 2) {
								int ln = t1.nextInt(LAST_NAMES.length);
								tempString2 += LAST_NAME + ":" + LAST_NAMES[ln]
										+ "; ";
							}

						} else if (tempString[j].split(":")[0].equals("")) {

							tempVar = t1.nextInt(3);
							if (tempVar == 1) {
								tempString2 += tempString[j] + "; ";
							} else if (tempVar == 2) {
								int ln = t1.nextInt(SSN.length);
								tempString2 += SOCIAL_SECURITY_NUMBER + ":"
										+ SSN[ln] + "; ";
							}
						}

						else if (tempString[j].split(":")[0].equals(DOB)) {

							tempVar = t1.nextInt(3);
							if (tempVar == 1) {
								tempString2 += tempString[j] + "; ";
							} else if (tempVar == 2 && tempVar3 < 2) {
								String date = generateDate();
								tempString2 += DOB + ":" + date + "; ";
								tempVar3++;
							} else {
								tempString2 += tempString[j] + "; ";
							}

						}

						else if (tempString[j].split(":")[0].equals(ADDRESS)) {

							tempVar = t1.nextInt(3);
							if (tempVar == 1) {
								tempString2 += tempString[j] + "; ";
							} else if (tempVar == 2) {
								int ln = t1.nextInt(STREET_ADDRESS.length);
								tempString2 += ADDRESS + ":"
										+ STREET_ADDRESS[ln] + ", ";
								ln = t1.nextInt(CITY.length);
								tempString2 += CITY[ln] + "; ";
							}
						}
						sCurrentLine = tempString2;

					}
				}
				int tempVar2 = 1;
				int v1 = t1.nextInt(2);
				if (v1 == 1) {
					int d = t1.nextInt(DISEASES.length);
					sCurrentLine = sCurrentLine + "Value" + tempVar2 + ":"
							+ DISEASES[d] + "; ";
					tempVar2++;
				}

				int v2 = t1.nextInt(2);
				if (v2 == 1) {
					int m = t1.nextInt(MEDICINES.length);
					sCurrentLine = sCurrentLine + "Value" + tempVar2 + ":"
							+ MEDICINES[m] + "; ";
					tempVar2++;
				}

				int v3 = t1.nextInt(2);
				if (v3 == 1) {
					int t = t1.nextInt(TREATMENT.length);
					sCurrentLine = sCurrentLine + "Value" + tempVar2 + ":"
							+ TREATMENT[t] + "; ";
					tempVar2++;
				}
				bw.write(sCurrentLine);

				br.close();

			} else {
				String temp = "";
				Random r = new Random();

				int fn = r.nextInt(FIRST_NAMES.length);
				temp = temp + FIRST_NAME + ":" + FIRST_NAMES[fn] + "; ";

				int ln = r.nextInt(LAST_NAMES.length);
				temp = temp + LAST_NAME + ":" + LAST_NAMES[ln] + "; ";

				int ssn = r.nextInt(SSN.length);
				temp = temp + SOCIAL_SECURITY_NUMBER + ":" + SSN[ssn] + "; ";

				String date = generateDate();

				temp = temp + DOB + ":" + date + "; ";

				int sa = r.nextInt(STREET_ADDRESS.length);
				temp = temp + ADDRESS + ":" + STREET_ADDRESS[sa] + ", ";

				int c = r.nextInt(CITY.length);
				temp = temp + CITY[c] + "; ";

				if (i == sameEntryNo) {

					File fout2 = new File("temp.txt");
					FileWriter fileWritter = new FileWriter(fout2.getName(),
							true);

					BufferedWriter bw2 = new BufferedWriter(fileWritter);
					bw2.write(temp);
					bw2.newLine();
					bw2.close();
				}
				int tempVar2 = 1;
				int v1 = r.nextInt(2);
				if (v1 == 1) {
					int d = r.nextInt(DISEASES.length);
					temp = temp + "Value" + tempVar2 + ":" + DISEASES[d] + "; ";
					tempVar2++;
				}

				int v2 = r.nextInt(2);
				if (v2 == 1) {
					int m = r.nextInt(MEDICINES.length);
					temp = temp + "Value" + tempVar2 + ":" + MEDICINES[m]
							+ "; ";
					tempVar2++;
				}

				int v3 = r.nextInt(2);
				if (v3 == 1) {
					int t = r.nextInt(TREATMENT.length);
					temp = temp + "Value" + tempVar2 + ":" + TREATMENT[t]
							+ "; ";
					tempVar2++;
				}

				bw.write(temp);
			}
			bw.newLine();
		}

		bw.close();
	}
}
