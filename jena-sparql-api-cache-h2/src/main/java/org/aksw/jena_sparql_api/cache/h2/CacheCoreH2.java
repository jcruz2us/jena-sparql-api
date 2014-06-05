package org.aksw.jena_sparql_api.cache.h2;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.aksw.commons.collections.SinglePrefetchIterator;
import org.aksw.commons.util.strings.StringUtils;
import org.aksw.jena_sparql_api.cache.core.QueryString;
import org.aksw.jena_sparql_api.cache.extra.CacheBackend;
import org.aksw.jena_sparql_api.cache.extra.CacheCore;
import org.aksw.jena_sparql_api.cache.extra.CacheCoreExCompressor;
import org.aksw.jena_sparql_api.cache.extra.CacheEntry;
import org.aksw.jena_sparql_api.cache.extra.CacheEntryBase;
import org.aksw.jena_sparql_api.cache.extra.CacheEntryImpl;
import org.aksw.jena_sparql_api.cache.extra.InputStreamProviderResultSetBlob;
import org.aksw.jena_sparql_api.cache.extra.SqlDaoBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class is used to cache information about resources to a database.
 * Provides the connection to an H2 database in a light weight, configuration free
 * manner.
 *
 * Important: This implementation of the CacheCoreEx interface uses combined hashes of services and
 * queries.
 *
 *
 * @author Jens Lehmann, Claus Stadler
 *
 */
public class CacheCoreH2
    extends SqlDaoBase
    implements CacheCore, CacheBackend
{
    private static final Logger logger = LoggerFactory.getLogger(CacheCoreH2.class);

    private boolean validateHash = true;
    
    private String defaultService = "";

    @Override
    public CacheEntry lookup(String queryString) {
        return lookup(defaultService, queryString);
    }

    @Override
    public void write(String queryString, InputStream in) {
        write(defaultService, queryString, in);
    }

    enum Query
        implements QueryString
    {
        CREATE("CREATE TABLE IF NOT EXISTS query_cache(query_hash BINARY PRIMARY KEY, query_string VARCHAR(15000), data BLOB, time TIMESTAMP)"),
        LOOKUP("SELECT * FROM query_cache WHERE query_hash=? LIMIT 1"),
        INSERT("INSERT INTO query_cache VALUES(?,?,?,?)"),
        UPDATE("UPDATE query_cache SET data=?, time=? WHERE query_hash=?"),
        DUMP("SELECT * FROM query_cache")
        ;

        private String queryString;

        Query(String queryString) { this.queryString = queryString; }
        public String getQueryString() { return queryString; }
    }


	private String databaseDirectory = "cache";
	private String databaseName = "extraction";
	private boolean autoServerMode = true;

	// specifies after how many milli seconds a cached result becomes invalid
	private long lifespan = 1 * 24 * 60 * 60 * 1000; // 1 day


	private Connection conn;


    public static CacheCoreH2 create(String dbName)
            throws ClassNotFoundException, SQLException
    {
        return create(true, "cache/sparql", dbName, 1 * 24 * 60 * 60 * 1000);
    }

    public static CacheCoreH2 create(String dbName, long lifespan)
            throws ClassNotFoundException, SQLException
    {
        return create(true, "cache/sparql", dbName, lifespan);
    }

    /**
     * Loads the driver
     */
    public static CacheCoreH2 create(boolean autoServerMode, String dbDir, String dbName, long lifespan)
            throws ClassNotFoundException, SQLException {
        return (CacheCoreH2)create(autoServerMode, dbDir, dbName, lifespan, false);
    }

    public static CacheBackend create(boolean autoServerMode, String dbDir, String dbName, long lifespan, boolean useCompression)
            throws ClassNotFoundException, SQLException
    {
            Class.forName("org.h2.Driver");

            String jdbcString = ";AUTO_RECONNECT=TRUE";
            if(autoServerMode) {
                jdbcString += ";AUTO_SERVER=TRUE";
            }

            // connect to database (created automatically if not existing)
            Connection conn = DriverManager.getConnection("jdbc:h2:" + dbDir + "/" + dbName + jdbcString, "sa", "");

            // create cache table if it does not exist
            Statement stmt = conn.createStatement();

            CacheCoreH2 tmp = new CacheCoreH2(conn, lifespan);

        return useCompression
            ? new CacheCoreExCompressor(tmp)
            : tmp;
    }

    public static CacheBackend create(String dbName, long lifespan, boolean useCompression)
            throws ClassNotFoundException, SQLException
    {
        return create(true, "cache/sparql", dbName, lifespan, useCompression);
    }

    
	public CacheCoreH2(Connection conn, long lifespan)
            throws SQLException
    {
        super(Arrays.asList(Query.values()));

        try {
            conn.createStatement().executeUpdate(Query.CREATE.getQueryString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        setConnection(conn);

        this.lifespan = lifespan;
    }

    /**
     * Note: Do not close the InputStream provider on the cache entries
     * as this closes the underlying result set.
     *
     * @return
     * @throws SQLException
     */
    public Iterator<CacheEntryImpl> iterator()
            throws SQLException
    {
        final ResultSet rs = executeQuery(Query.DUMP);

        return new SinglePrefetchIterator<CacheEntryImpl>() {

            @Override
            protected CacheEntryImpl prefetch() throws Exception {
                if(!rs.next()) {
                    return null;
                }
                byte[] rawQueryHash = rs.getBytes("query_hash");

                String queryHash = StringUtils.bytesToHexString(rawQueryHash);
                String queryString = rs.getString("query_string");
                Timestamp timestamp = rs.getTimestamp("time");
                Blob data = rs.getBlob("data");

                return new CacheEntryImpl(timestamp.getTime(), lifespan, new InputStreamProviderResultSetBlob(rs, data).open(), queryString, queryHash);
                //return new CacheEntryImpl(timestamp.getTime(), lifespan, new InputStreamProviderResultSetBlob(rs, data), queryString, queryHash);
//                if(true) {
//                    throw new RuntimeException("Deprecated; do not use");
//                }

//                return null;
            }

            @Override
            public void close() {
                try {
                    rs.close();
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public synchronized CacheEntry lookup(String service, String queryString)
    {
        try {
            return _lookup(service, queryString);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public CacheEntry _lookup(String service, String queryString)
            throws SQLException
    {
        String md5 = StringUtils.md5Hash(createHashRoot(service, queryString));
        //String md5 = StringUtils.md5Hash(queryString);

        ResultSet rs = executeQuery(Query.LOOKUP, md5);

        try {
            if(rs.next()) {
                Timestamp timestamp = rs.getTimestamp("time");
                Blob data = rs.getBlob("data");

                if(validateHash) {
                    String cachedQueryString = rs.getString("query_string");

                    if(!cachedQueryString.equals(queryString)) {
                        logger.error("HASH-CLASH:\n" + "Service: " + service + "\nNew QueryString: " + queryString + "\nOld QueryString: " + cachedQueryString);
                        return null;
                    }
                }

                return new CacheEntryBase(timestamp.getTime(), lifespan, new InputStreamProviderResultSetBlob(rs, data).open());
//                if(true) {
//                    throw new RuntimeException("Deprecated; do not use");
//                }
            }

            if(rs.next()) {
                logger.warn("Multiple cache hits found, just one expected.");
            }
        } finally {
            //Note we must not close the rs here - the InputStreamProvider of the CacheEntry must be closed
            //SqlUtils.close(rs);
        }

        return null;
    }

    public synchronized void write(String service, String queryString, InputStream in) {
        try {
            _write(service, queryString, in);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void _write(String service, String queryString, InputStream in)
            throws SQLException
    {
        String md5 = StringUtils.md5Hash(createHashRoot(service, queryString));
        //String md5 = StringUtils.md5Hash(queryString);

        Timestamp timestamp = new Timestamp(new GregorianCalendar().getTimeInMillis());


        //Reader reader = new InputStreamReader(in);

        ResultSet rs = null;
        try {
            rs = executeQuery(Query.LOOKUP, md5);

            if(rs != null && rs.next()) {
                execute(Query.UPDATE, null, in, timestamp, md5);
            } else {
                execute(Query.INSERT, null, md5, queryString, in, timestamp);
            }
        } finally {
            if(rs != null) {
                rs.close();
            }
        }

        //return lookup(queryString);
    }

    public String createHashRoot(String service, String queryString) {
        return service + queryString;
    }

}
