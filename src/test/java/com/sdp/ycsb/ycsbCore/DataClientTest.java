package com.sdp.ycsb.ycsbCore;

import com.sdp.log.Log;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.db.DataClient;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 * DataClient Tester.
 *
 * @author magq
 * @version 1.0
 * @since <pre>Nov 2, 2016</pre>
 */
public class DataClientTest {

    private DataClient dataClient = new DataClient();

    @Before
    public void before() throws Exception {
        Log.init();
        String configPath = System.getProperty("user.dir") + "/workloads/workload";
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configPath));
            dataClient.setProperties(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        dataClient.init();
    }

    @After
    public void after() throws Exception {
        dataClient.cleanup();
    }

    /**
     * Method: init()
     */
    @Test
    public void testInit() throws Exception {
    }

    /**
     * Method: cleanup()
     */
    @Test
    public void testCleanup() throws Exception {
    }

    /**
     * Method: read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result)
     */
    @Test
    public void testRead() throws Exception {
        String table = "test";
        String key = "user001";
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        dataClient.read(table, key, null, values);
    }

    /**
     * Method: update(String table, String key, HashMap<String, ByteIterator> values)
     */
    @Test
    public void testUpdate() throws Exception {
        String table = "test";
        String key = "user001";
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        values.put("1", new RandomByteIterator(1));

        dataClient.update(table, key, values);
    }
} 
