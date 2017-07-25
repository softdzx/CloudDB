package storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.*;

import logger.LogSetup;

/**
 * Persistence class to handle the storage of data on the disk.
 */
public class Persistance extends Storage {
	private String fileName = null;
//	private BufferedReader reader;
//	private static Logger logger = Logger.getRootLogger();

	public Persistance(String name) {
		try {
			this.fileName = "persisteddata" + name;
			File file = new File(fileName);
			file.createNewFile();
			file.setWritable(true);
		} catch (IOException e) {
			Logger.getRootLogger().error("Persistance error.", e);
		}
	}

	/**
	 * Stores a key-value-pair in the storage file.
	 * 
	 * @param key
	 *            The key to put
	 * @param value
	 *            The value to put
	 */
	public synchronized void put(String key, String value) {
		try {
			File file = new File(fileName);
			FileWriter fw = new FileWriter(file, true);
			BufferedWriter writer = new BufferedWriter(fw);

			if (get(key) != null) {
				remove(key);
			}
			writer.write(key + ',' + value + "\n");
			writer.flush();
			writer.close();
			fw.close();
		} catch (Exception e) {
			Logger.getRootLogger().error("Data storage error.", e);
		}
	}

	public synchronized ArrayList<KeyValue> getAllPairs() {
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		try {
			File file = new File(fileName);
			FileReader fr = new FileReader(file);
			BufferedReader reader = new BufferedReader(fr);
			String temp = null;
			while ((temp = reader.readLine()) != null) {
				String[] kvpair = temp.split(",", 2);
				if (kvpair.length == 2) {
					results.add(new KeyValue(kvpair[0], kvpair[1]));
				}
			}
			reader.close();
			reader=null;
			fr.close();			
		} catch (IOException e) {
			Logger.getRootLogger().error("Data lookup error.", e);
		}
		return results;
	}

	public synchronized ArrayList<KeyValue> get(String from, String to) {
		ArrayList<KeyValue> results = new ArrayList<KeyValue>();
		for (KeyValue keyvalue : getAllPairs()) {
			if (inRange(keyvalue.getKey(), from, to))
				results.add(new KeyValue(keyvalue.getKey(), keyvalue.getValue()));
		}
		return results;
	}

	public synchronized void remove(String from, String to) {
		for (KeyValue keyvalue : getAllPairs()) {
			if (inRange(keyvalue.getKey(), from, to))
				remove(keyvalue.getKey());
		}
	}

	public synchronized String get(String key) {
		String keyvalue;
		try {
			File file = new File(fileName);
			FileReader fr = new FileReader(file);
			BufferedReader reader = new BufferedReader(fr);

			while ((keyvalue = reader.readLine()) != null) {

				String[] kvpair = keyvalue.split(",", 2);
				if (kvpair != null && kvpair[0].equals(key)) {
					reader.close();
					reader=null;
					fr.close();
					return kvpair[1];
				}
			}
			reader.close();
			reader=null;
			fr.close();

		} catch (IOException e) {
			Logger.getRootLogger().error("Data lookup error.", e);
		}

		return null;
	}

	/**
	 * Removes a data record with a certain key from the storage file.
	 * 
	 * @param key
	 *            The key to be looked for.
	 * @return A String documenting the results.
	 */
	public synchronized String remove(String key) {
		String value = null;
		try {
			File file = new File(fileName);
			FileReader fr = new FileReader(file);
			BufferedReader reader = new BufferedReader(fr);

			File tempFile = new File("persisteddata.tmp");
			FileWriter fw = new FileWriter(tempFile);
			BufferedWriter tempWriter = new BufferedWriter(fw);
			String currentLine;
			
			while ((currentLine = reader.readLine()) != null) {

				if (currentLine.trim().split(",")[0].equals(key)) {
					value = currentLine.trim().split(",")[1];
					continue;
				} else {
					tempWriter.write(currentLine + "\n");
					tempWriter.flush();
				}
			}
			
			tempWriter.close();
			reader.close();
			fw.close();
			fr.close();
			
			file.delete();
			tempFile.renameTo(file);

		} catch (FileNotFoundException e) {
			Logger.getRootLogger().error("Data storage file not found.", e);
		} catch (IOException e) {
			Logger.getRootLogger().error("Data storage error.", e);
		}
		return value;
	}

}