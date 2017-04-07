package com.couchbase.util;


import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.core.node.*;
import com.couchbase.client.core.service.*;
import com.couchbase.client.core.endpoint.*;
import com.couchbase.client.java.util.rawQuerying.*;
import static com.couchbase.client.java.query.dsl.Expression.*;
import com.couchbase.client.java.query.Delete;
import com.couchbase.client.java.query.dsl.path.DefaultDeleteUsePath;

import com.google.gson.Gson;
import rx.*;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


public class SqlImporter {


    private static final String COUCHBASE_URIS = "cb.uris";
    private static final String COUCHBASE_BUCKET = "cb.bucket";
    private static final String COUCHBASE_PASSWORD = "cb.password";

    private static final String SQL_CONN_STRING = "sql.connection";
    private static final String SQL_USER = "sql.username";
    private static final String SQL_PASSWORD = "sql.password";

    private static final String TABLES_LIST = "import.tables";
    private static final String CREATE_VIEWS = "import.createViews";
    private static final String TYPE_FIELD = "import.typefield";
    private static final String TYPE_CASE = "import.fieldcase";

    private CouchbaseClient couchbaseClient = null;
    private Connection connection = null;

    // JDBC connection
    private String sqlConnString = null;
    private String sqlUser = null;
    private String sqlPassword = null;

    // Couchbase information
    private List uris = new ArrayList();
    private String bucketName = "default";
    private String defaultUri = "http://127.0.0.1:8091/pools";
    private String password = "";
    
    private Cluster cluster;
    private Bucket bucket;
    
    private DesignDocument[] designDoc = new DesignDocument[4];

    // import options
    private String typeField = null;
    private String typeFieldCase = null;
    private boolean createTableViewEnable = true;
    private String[] tableList = null;

    
    private int tableIndex = 0;
    public static void main(String[] args) {
        System.out.println("\n\n");
        System.out.println("############################################");
        System.out.println("#         COUCHBASE SQL IMPORTER           #");
        System.out.println("############################################\n\n");

        SqlImporter importer = new SqlImporter();
        try {

            // remove log info from Couchbase
            Properties systemProperties = System.getProperties();
            systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
            System.setProperties(systemProperties);
            Logger logger = Logger.getLogger("com.couchbase.client");
            logger.setLevel(Level.WARNING);
            for (Handler h : logger.getParent().getHandlers()) {
                if (h instanceof ConsoleHandler) {
                    h.setLevel(Level.WARNING);
                }
            }
            importer.setup();
            importer.createDesignDocument();
            importer.importData();
            importer.addDesignDoc();
            importer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("\n\n              FINISHED");
        System.out.println("############################################");
        System.out.println("\n\n");


    }

    private void setup()
    {    	
        try {
        	String resourceName = "sample-import.properties"; 
        	ClassLoader loader = Thread.currentThread().getContextClassLoader();
        	Properties prop = new Properties();
        	InputStream resourceStream = loader.getResourceAsStream(resourceName);
        	prop.load(resourceStream);

            if (prop.containsKey(COUCHBASE_URIS)) {
                String[] uriStrings = prop.getProperty(COUCHBASE_URIS).split(",");
                for (int i = 0; i < uriStrings.length; i++) {
                    uris.add(new URI(uriStrings[i]));
                }
            } else {
                uris.add(new URI("http://127.0.0.1:8091/pools"));
            }

            if (prop.containsKey(COUCHBASE_BUCKET)) {
            	bucketName = prop.getProperty(COUCHBASE_BUCKET);
            }

            if (prop.containsKey(COUCHBASE_PASSWORD)) {
                password = prop.getProperty(COUCHBASE_PASSWORD);
            }

            if (prop.containsKey(SQL_CONN_STRING)) {
                sqlConnString = prop.getProperty(SQL_CONN_STRING);
            } else {
                throw new Exception(" JDBC Connection String not specified");
            }

            if (prop.containsKey(SQL_USER)) {
                sqlUser = prop.getProperty(SQL_USER);
            } else {
                throw new Exception(" JDBC User not specified");
            }

            if (prop.containsKey(SQL_PASSWORD)) {
                sqlPassword = prop.getProperty(SQL_PASSWORD);
            } else {
                throw new Exception(" JDBC Password not specified");
            }

            if (prop.containsKey(TABLES_LIST)) {
                tableList = prop.getProperty(TABLES_LIST).split(",");
            }

            if (prop.containsKey(CREATE_VIEWS)) {
                createTableViewEnable = Boolean.parseBoolean(prop.getProperty(CREATE_VIEWS));
            }

            if (prop.containsKey(TYPE_FIELD)) {
                typeField = prop.getProperty(TYPE_FIELD);
            }

            if (prop.containsKey(TYPE_CASE)) {
                typeFieldCase = prop.getProperty(TYPE_CASE);
            }
            System.out.println("\nImporting table(s)");
            System.out.println("\tfrom : \t" + sqlConnString);
            System.out.println("\tto : \t" + uris + " - " + bucketName);
            
            CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
            	    .socketConnectTimeout((int) TimeUnit.SECONDS.toMillis(45))
            	    .connectTimeout(TimeUnit.SECONDS.toMillis(60))
            	    .build();
            System.setProperty("com.couchbase.queryEnabled", "true");
            cluster = CouchbaseCluster.create(env,uris);
            bucket = cluster.openBucket(bucketName);

        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n\n");
            System.exit(0);
        }
    }

    private void shutdown() throws SQLException {
        if (couchbaseClient != null) {
            couchbaseClient.shutdown(5, TimeUnit.SECONDS);
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }


    public SqlImporter() {
    }


    public void importData() throws Exception {
        if (tableList == null || tableList[0].equalsIgnoreCase("ALL")) {
            this.importAllTables();
        } else {
            for (int i = 0; i < tableList.length; i++) {
                this.importTable(tableList[i].trim());
            }
        }
    }

    public void importAllTables() throws Exception {
        DatabaseMetaData md = this.getConnection().getMetaData();
        ResultSet rs = md.getTables(null, null, "%", new String[] {"TABLE"});
        int i =0;
        while (rs.next()) {
        	String tableType = rs.getString("TABLE_TYPE");
        	String tableName = rs.getString(3);
        	if(tableType.equals("TABLE"))
        		importTable(tableName);
        	if(i > 10) break;
        	i++;
        }
    }

    public void importTable(String tableName) throws Exception {
    	
        System.out.println("\n  Exporting Table : " + tableName);
        if (createTableViewEnable) {
            this.createViewsForPrimaryKey(tableName);
        }
        
        com.couchbase.client.java.query.Statement statement = Delete.deleteFrom(i(bucketName).as("t")).where("meta(t).id like '"+tableName+"%'");
    	JsonObject placeholderValues = JsonObject.create();
    	bucket.query(N1qlQuery.parameterized(statement, placeholderValues));
        PreparedStatement preparedStatement = null;
        boolean isLoaded = false;
        int offsetIndex = 0, limitCount = 1000;
        Gson gson = new Gson();
        
        int rowCount = 0;
        try {
        	while(!isLoaded){
        		List<String> withoutId = new ArrayList<String> ();
        		withoutId.add("campaigns_order");
        		withoutId.add("clientdetails_conversation_time");
        		withoutId.add("clientdetails_relation");
        		withoutId.add("clientdetails_relation_type");
        		withoutId.add("clientdetails_opening_time");
        		withoutId.add("middleware_geolocation");
        		withoutId.add("middleware_key_value_storage");
        		withoutId.add("orders_orders");
        		withoutId.add("users");
        		
        		String selectSQL = "";
        		if(!withoutId.contains(tableName))
        			selectSQL = "SELECT * FROM " + tableName + " ORDER BY id OFFSET "+offsetIndex+" ROWS FETCH NEXT "+limitCount+" ROWS ONLY";
        		else{
        			selectSQL = "SELECT * FROM " + tableName;
        			isLoaded = true;
        		}
                preparedStatement = this.getConnection().prepareStatement(selectSQL);
                ResultSet rs = preparedStatement.executeQuery();
                ResultSetMetaData rsmd = rs.getMetaData();
                int numColumns = rsmd.getColumnCount();
                List<JsonDocument> documents = new ArrayList<JsonDocument>();
                int fetchCount = 0;
                
                while (rs.next()) {
                	JsonObject content = JsonObject.create();
                    for (int i = 1; i < numColumns + 1; i++) {
                        String columnName = this.getNamewithCase(rsmd.getColumnName(i), typeFieldCase);
                        if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
                        	content.put(columnName, rs.getArray(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
                        	content.put(columnName, rs.getInt(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
                        	content.put(columnName, rs.getBoolean(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
                        	content.put(columnName, rs.getBlob(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
                        	content.put(columnName, rs.getDouble(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
                        	content.put(columnName, rs.getFloat(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
                        	content.put(columnName, rs.getInt(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
                        	content.put(columnName, rs.getNString(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
                        	content.put(columnName, rs.getString(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
                        	content.put(columnName, rs.getInt(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
                        	content.put(columnName, rs.getInt(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
                        	content.put(columnName, rs.getDate(columnName));
                        } else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
                        	content.put(columnName,String.valueOf(rs.getTimestamp(columnName)));
                        }else if (rsmd.getColumnType(i) == java.sql.Types.TIME) {
                        	content.put(columnName,String.valueOf(rs.getTime(columnName)));
                        }else {
                        	content.put(columnName, rs.getObject(columnName));
                        }
                    }
                    if(content.isEmpty()) continue;
                    rowCount ++;
                    documents.add(JsonDocument.create(tableName+":"+rowCount, content));
                    fetchCount = rs.getRow();
                }
                if(fetchCount < (limitCount))
                	isLoaded = true;
                System.out.print(" : "+gson.toJson(documents) + "\n\n");
                if(documents.isEmpty()){
                	isLoaded = true;
                	break;
                }
                
                rx.Observable.from(documents)
                    .flatMap(new Func1<JsonDocument, rx.Observable<JsonDocument>>() {
        				public Observable<JsonDocument> call(JsonDocument t) {
        					return bucket.async().insert(t);
        				}
                    })
                    .last()
                    .toBlocking()
                    .single();
               offsetIndex += limitCount;
        	}
            System.out.println("All records of " + tableName + " moved to Couchbase.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    @SuppressWarnings({ "rawtypes" })
	private void createDesignDocument() throws Exception
    {
    	String []designNames = {"designDocument1","designDocument2","designDocument3","designDocument4"};
    	for(int i = 0; i < designNames.length; i ++)
    	{
    		designDoc[i] = new DesignDocument(designNames[i]);
    	}
    }
    
    @SuppressWarnings("unchecked")
	private void createViewsForPrimaryKey(String tableName) {
        String typeName = this.getNamewithCase(tableName, typeFieldCase);
		List<String> pkCols = new ArrayList<String>();
        DatabaseMetaData databaseMetaData = null;
        try {
            databaseMetaData = this.getConnection().getMetaData();
            ResultSet rs = databaseMetaData.getPrimaryKeys(null, null, tableName);
            while (rs.next()) {
                pkCols.add(rs.getString(4));
            }
            String[] array = pkCols.toArray(new String[pkCols.size()]);

            StringBuilder mapFunction = new StringBuilder();
            StringBuilder ifStatement = new StringBuilder();
            StringBuilder emitStatement = new StringBuilder();


            mapFunction.append("function (doc, meta) {\n")
                       .append("  var idx = (meta.id).indexOf(\":\");\n")
                       .append("  var docType = (meta.id).substring(0,idx); \n");


            if (array != null && array.length == 1) {
                ifStatement.append("  if (meta.type == 'json' && docType == '")
                           .append(typeName)
                           .append("' ){ \n");
                emitStatement.append("    emit(doc.").append(array[0]).append(");");
            } else if (array != null && array.length > 1) {
                emitStatement.append("    emit([");
                ifStatement.append("  if (meta.type == 'json' && docType == '")
                           .append(typeName);

                for (int i = 0; i < array.length; i++) {
                    emitStatement.append("doc.").append( this.getNamewithCase( array[i], typeFieldCase) );
                    if (i < (array.length-1)) {
                      emitStatement.append(", ");
                    }
                }
                ifStatement.append("' ){\n");
                emitStatement.append("]);\n");
            }else
            {
            	return;
            }

            mapFunction.append( ifStatement )
                       .append(emitStatement)
                       .append("  }\n")
                       .append("}\n");

            System.out.println("\n\n Create Couchbase views for table "+ typeName);
            String viewName = typeName;
            String reduceFunction = "";
            ViewDesign viewDesign = new ViewDesign(viewName, mapFunction.toString(), reduceFunction);
            
            designDoc[tableIndex/7].getViews().add(viewDesign);
            tableIndex++;

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addDesignDoc() throws Exception
    {
    	 for(int i = 0; i < designDoc.length; i ++)
         {
    		if(designDoc[i].getViews().size() > 0)
    			getCouchbaseClient().createDesignDoc(designDoc[i]);
         }
    }

    public CouchbaseClient getCouchbaseClient() throws Exception {
        if (couchbaseClient == null) {
            couchbaseClient = new CouchbaseClient(uris, bucketName, password);
        }
        return couchbaseClient;
    }

    public void setCouchbaseClient(CouchbaseClient couchbaseClient) {
        this.couchbaseClient = couchbaseClient;
    }

    public Connection getConnection() throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(sqlConnString, sqlUser, sqlPassword);
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    private String getNamewithCase(String tablename, String nameType) {
        String returnValue =  tablename;
        if (nameType.equalsIgnoreCase("lower")) {
            returnValue = tablename.toLowerCase();
        } else if (nameType.equalsIgnoreCase("upper")) {
            returnValue = tablename.toUpperCase();
        }
        return returnValue;
    }
}
