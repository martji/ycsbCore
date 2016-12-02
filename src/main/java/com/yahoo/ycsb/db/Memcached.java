// Jacob Leverich <leverich@stanford.edu>, 2011
// Memcached client for YCSB framework.
//
// Properties:
//   memcached.server=memcached.xyz.com
//   memcached.port=11211

package com.yahoo.ycsb.db;

import com.sdp.server.ServerNode;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Memcached extends DB {

    private MemcachedClient client;
    private Properties props;

    public final int OK = 0;
    public final int ERROR = -1;
    public final int NOT_FOUND = -2;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        props = getProperties();

        String serverPath = props.getProperty("server_path");
        if (serverPath == null) {
            throw new DBException("server_path param must be specified");
        }
        List<ServerNode> nodes = getServerNode(System.getProperty("user.dir") + serverPath);
        List<InetSocketAddress> address = new ArrayList<InetSocketAddress>();
        try {
            for (int i = 0; i < nodes.size(); i++) {
                String host = nodes.get(i).getHost();
                int port = nodes.get(i).getDataPort();
                address.add(new InetSocketAddress(host, port));
            }
            client = new MemcachedClient(address);
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Cleanup any state for this DB. Called once per DB instance; there is one
     * DB instance per client thread.
     */
    public void cleanup() throws DBException {
        if (client.isAlive())
            client.shutdown();
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String values = (String) client.get(table + ":" + key);
        if (values == null)
            return NOT_FOUND;
        if (values.length() == 0)
            return NOT_FOUND;

        result.put(key, new ByteArrayByteIterator(values.getBytes()));
        return OK;
    }

    /**
     * Perform a range scan for a set of records in the database. Each
     * field/value pair from the result will be stored in a HashMap.
     *
     * @param table       The name of the table
     * @param startkey    The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields      The list of fields to read, or null for all of them
     * @param result      A Vector of HashMaps, where each HashMap is a set field/value
     *                    pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return ERROR;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key, overwriting any existing values with the same field name.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();
        for (String k : values.keySet()) {
            new_values.put(k, values.get(k).toArray());
        }

        OperationFuture<Boolean> f = client.set(table + ":" + key, 3600 * 24 * 15, new_values.toString());

        try {
            return f.get() ? OK : ERROR;
        } catch (InterruptedException e) {
            return ERROR;
        } catch (ExecutionException e) {
            return ERROR;
        }
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified record
     * key.
     *
     * @param table  The name of the table
     * @param key    The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    public int insert(String table, String key,
                      HashMap<String, ByteIterator> values) {
        return update(table, key, values);
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key   The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's
     * description for a discussion of error codes.
     */
    public int delete(String table, String key) {
        client.delete(table + ":" + key);
        return OK; // FIXME check future
    }

    public List<ServerNode> getServerNode(String serverListPath) {
        List<ServerNode> list = new ArrayList<ServerNode>();
        SAXReader sr = new SAXReader();
        try {
            Document doc = sr.read(serverListPath);
            Element root = doc.getRootElement();
            List<Element> childElements = root.elements();
            for (Element server : childElements) {
                int id = Integer.parseInt(server.elementText("id"));
                String host = server.elementText("host");
                int readPort = Integer.parseInt(server.elementText("readPort"));
                int writePort = Integer.parseInt(server.elementText("writePort"));
                int dataPort = Integer.parseInt(server.elementText("dataPort"));
                ServerNode serverNode = new ServerNode(id, host, readPort, writePort, dataPort);
                list.add(serverNode);
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return list;
    }
}
