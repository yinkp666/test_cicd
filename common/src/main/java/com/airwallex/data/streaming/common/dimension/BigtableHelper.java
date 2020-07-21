package com.airwallex.data.streaming.common.dimension;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class BigtableHelper {
    private static final String PROJECT_ID = "cn-data-engineering-playground";
    private static final String INSTANCE_ID = "awx-bigtable";
    // Include the following line if you are using app profiles.
    // If you do not include the following line, the connection uses the
    // default app profile.
    private static final String APP_PROFILE_ID = "/Users/jim.yang/Documents/kafka_2.12-2.3.0/config/client_secret.json";

    private static Connection connection = null;

    public static void connect() throws IOException {
        Configuration config = BigtableConfiguration.configure(PROJECT_ID, INSTANCE_ID);
        // Include the following line if you are using app profiles.
        // If you do not include the following line, the connection uses the
        // default app profile.
        config.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY, APP_PROFILE_ID);

        connection = BigtableConfiguration.connect(config);
    }
    public static  void  main(String[] args) throws IOException {
//        connect();
        Configuration config = HBaseConfiguration.create();
        connection= BigtableConfiguration.connect(config);
        connection.getTable(TableName.valueOf("test_profile"));
        System.out.println(StringUtils.join(connection.getAdmin().listTableNames(),","));
    }
}
