package org.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MultiOpt {

    public static Configuration configuration;
    static Admin admin = null;
    static Connection connection = null;

    public static void main(String[] args) {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            // todo opt
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void createTable(String tableName, String[] fields) throws IOException {
        TableName table = TableName.valueOf(tableName);
        // 判断是否存在列族信息
        if (fields.length == 0) {
            System.out.println("请设置列族信息！");
            return;
        }
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        }
        //判断表是否存在
        //创建表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
        //创建多个列族
        for (String cf : fields) {
            // 创建列族描述器
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        //根据对表的配置，创建表
        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("表" + tableName + "创建成功！");
    }

    void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
            Put put = new Put(row.getBytes());
            String[] cols = fields[i].split(":");
            put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            table.put(put);
        }
        table.close();
    }

    void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(column));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)));
                System.out.println("Timestamp:" + cell.getTimestamp());
                System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)));
                System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + new String(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    void modifyData(String tableName, String row, String column, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(row.getBytes());
        Scan scan = new Scan();
        long ts = 0;
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            for (Cell cell : r.getColumnCells(row.getBytes(), column.getBytes())) {
                ts = cell.getTimestamp();
            }
        }
        put.addColumn(row.getBytes(), column.getBytes(), ts, val.getBytes());
        table.put(put);
        table.close();
    }

    void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(row.getBytes());
        table.delete(delete);
        table.close();
    }
}
