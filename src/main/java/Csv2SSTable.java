import java.nio.ByteBuffer;
import java.io.*;
import java.util.UUID;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class Csv2SSTable {
	static String filename;
	static String outdir;
	static String keyspace = "stest";
	static String table = "test1";
	static String delimiter = ",";
	static String schema = "CREATE TABLE " + keyspace + "." + table + " (partkey  TEXT, tstamp INT, col1 INT, col2 INT, col3 INT, col4 INT, col5 INT, col6 INT, col7 INT, col8 INT, PRIMARY KEY ((partkey), tstamp));";
	static String insert = "INSERT INTO " + keyspace + "." + table + " (partkey, tstamp, col1, col2, col3, col4, col5, col6, col7, col8) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("Expecting 2 arguments: <filename> <output directory>");
			System.exit(1);
		}

		filename = args[0];
		outdir = args[1];

                BufferedReader reader = new BufferedReader(new FileReader(filename));
                File directory = new File(keyspace);
                if (!directory.exists())
                        directory.mkdir();

                CsvParser parser = new CsvParser();

		CQLSSTableWriter writer = CQLSSTableWriter.builder().inDirectory(outdir).forTable(schema).using(insert).build();

                String line;
                int lineNumber = 1;

                long timestamp = System.currentTimeMillis() * 1000;
                while ((line = reader.readLine()) != null) {
			if (parser.parse(line, delimiter, lineNumber)) {
				parser.addRow(writer);
			}
                        lineNumber++;
                }
                writer.close();
                System.exit(0);
        }

	static class CsvParser {
		public String[] colnames;
		public String partkey;
		public int tstamp;
		public int col1;
		public int col2;
		public int col3;
		public int col4;
		public int col5;
		public int col6;
		public int col7;
		public int col8;
		static int numCols = 10;

		boolean parse(String line, String delimiter, int lineNumber) {
			String[] columns = line.split(delimiter);
			if (10 != columns.length) {
				System.err.println(String.format("Invalid input '%s' at line %d of %s", line, lineNumber, filename));
				return false;
			}
			try {
				partkey = columns[0].trim();
				tstamp = Integer.parseInt(columns[1].trim());
				col1 = Integer.parseInt(columns[2].trim());
                                col2 = Integer.parseInt(columns[3].trim());
                                col3 = Integer.parseInt(columns[4].trim());
                                col4 = Integer.parseInt(columns[5].trim());
                                col5 = Integer.parseInt(columns[6].trim());
                                col6 = Integer.parseInt(columns[7].trim());
                                col7 = Integer.parseInt(columns[8].trim());
                                col8 = Integer.parseInt(columns[9].trim());
				return true;
			}
			catch (NumberFormatException e) {
				System.err.println(String.format("Invalid number in input '%s' at line %d of %s", line, lineNumber, filename));
				return false;
			}
		}

		void addRow(CQLSSTableWriter writer) {
			try {
				writer.addRow(partkey, tstamp, col1, col2, col3, col4, col5, col6, col7, col8);
			}
			catch (Exception e) {
				System.err.println("Couldn't addRow");
			}
		}
	}
}

