package org.jdbcsqltest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

	private static final String CONFIG_FILE = "config.file";
	private static final String EXECUTE_QUERIES_ONLY = "execute.queries.only";
	private static final String CLEAN_DB_ONLY = "clean.db.only";
	
	private static Properties readCommandLineArguments(String[] args) {

		Properties props = new Properties();
		props.put(CONFIG_FILE, "./config.txt");
		props.put(EXECUTE_QUERIES_ONLY, new Boolean(false));
		props.put(CLEAN_DB_ONLY, new Boolean(false));
		
		if(args.length > 0){
			for (int i = 0; i < args.length; i++) {
				if (args[i].startsWith("-c")) {
					props.put(CLEAN_DB_ONLY, new Boolean(true));
				} else if (args[i].startsWith("-q")) {
					props.put(EXECUTE_QUERIES_ONLY, new Boolean(true));
				} else {
					props.put(CONFIG_FILE, args[i]);
				}
			}

		}
		return props;
	}
	
	private static void printDBInfo(Config config){
		Connection conn = null;

		System.out.println("\nDatabase Info.");
		try {
			conn = Util.getConnection(config);
			DatabaseMetaData dbmeta = conn.getMetaData();
			System.out.println("\tDatabase Product    : " + dbmeta.getDatabaseProductName());
			System.out.println("\tDatabase Version    : " + dbmeta.getDatabaseProductVersion());
			System.out.println("\tJDBC Driver Name    : " + dbmeta.getDriverName());
			System.out.println("\tJDBC Driver Version : " + dbmeta.getDriverVersion());			
		} catch (SQLException e) {
			throw new IllegalStateException("Cannot access database metadata " + e.getLocalizedMessage());
		} finally {
			try {
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				// ignore.
			}
		}
		System.out.println("Database Info Finished.");
	}
	
	public static void main(String[] args) {

		long startTime = System.currentTimeMillis();
        
		Properties cmdLineProps = readCommandLineArguments(args);
		
		Config.Initialize(cmdLineProps.getProperty(CONFIG_FILE));
		
		Config config = Config.getInstance();
		config.printConfig();
		
		printDBInfo(config);
		
        

		System.out.println("\nTest ended, Total time taken = " + (System.currentTimeMillis() - startTime) + " ms ");
	}
}

