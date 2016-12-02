package com.yahoo.ycsb.db;

import com.sdp.client.DBClient;
import com.sdp.common.RegisterHandler;
import com.sdp.server.ServerNode;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.util.*;

/**
 * Created by Guoqing on 2016/12/1.
 */
public class Ember extends DB {

    private DBClient client;
    private Properties props;

    public final int OK = 0;
    public final int ERROR = -1;
    public final int NOT_FOUND = -2;

    public void init() throws DBException {
        props = getProperties();
        String serverPath = props.getProperty("server_path");
        int type = Integer.decode(props.getProperty("client_type", "0"));
        int recordCount = Integer.decode(props.getProperty("recordcount"));
        int dataHashMode = Integer.decode(props.getProperty("data_hash_mode", "0"));
        int dataSetMode = Integer.decode(props.getProperty("data_set_mode", "0"));

        if (serverPath == null) {
            throw new DBException("server_path param must be specified");
        }

        RegisterHandler.initHandler();
        List<ServerNode> nodes = getServerNode(System.getProperty("user.dir") + serverPath);
        try {
            client = new DBClient(type, nodes);
            client.initConfig(recordCount, dataHashMode, dataSetMode);
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    public void cleanup() {
        client.shutdown();
    }

    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String values = client.get(table + ":" + key);
        if (values == null)
            return NOT_FOUND;
        if (values.length() == 0)
            return NOT_FOUND;

        result.put(key, new ByteArrayByteIterator(values.getBytes()));
        return OK;
    }

    public int scan(String table, String startkey, int recordcount,
                    Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return ERROR;
    }

    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();
        for (String k : values.keySet()) {
            new_values.put(k, values.get(k).toArray());
        }

        boolean out = client.set(table + ":" + key, new_values.toString());

        try {
            return out ? OK : ERROR;
        } catch (Exception e) {
            return ERROR;
        }
    }

    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return update(table, key, values);
    }

    public int delete(String table, String key) {
        return 0;
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
