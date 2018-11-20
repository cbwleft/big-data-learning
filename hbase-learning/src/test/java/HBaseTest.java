import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTest {

    private static final String ZOOKEEPER_HOST = "ubuntu";
    private static final String TABLE_NAME = "test";
    private static final String COLUMN_FAMILY = "cf";

    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;

    @BeforeClass
    public static void beforeClass() throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    @Test
    public void testCreateTable() throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        if (admin.tableExists(tableName)) {
            System.out.println("table exists!");
        } else {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY.getBytes()).build();
            TableDescriptor hTableDesc = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(columnFamilyDescriptor).build();
            admin.createTable(hTableDesc);
        }
    }

    @Test
    public void testPut() throws IOException {
        testCreateTable();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Put put = new Put(Bytes.toBytes("row-" + i));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("greet"), Bytes.toBytes("Hello"));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("person"), Bytes.toBytes("John"));
            puts.add(put);
        }
        table.put(puts);
    }

    @Test
    @Ignore
    public void testGet() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Get get = new Get(Bytes.toBytes("row-10"));
        Result result = table.get(get);
        byte[] val = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("greet"));
        Assert.assertArrayEquals("Hello".getBytes(), val);
    }

    @Test
    public void testScan() throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("greet"));
        scan.withStartRow(Bytes.toBytes("row-1"));
        scan.withStopRow(Bytes.toBytes("row-20"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            System.out.println("Row Value" + res);
        }
    }

    @Test
    @Ignore
    public void testDropTable() throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

}
