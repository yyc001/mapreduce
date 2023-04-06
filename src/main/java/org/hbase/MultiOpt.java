package org.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class MultiOpt {

    public static Configuration configuration;
    static Admin admin = null;
    static Connection connection = null;

    public static void main(String[] args) {
        System.out.println("Wait...");
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            if(args.length < 2){
                System.out.println("Input params:");
                Scanner scan = new Scanner(System.in);
                args = scan.nextLine().split(" ");
            }
            System.out.println("Processing..");
            String tableName;
            String row;
            String column;
            String value;
            String[] fields;
            String[] values;
            switch (args[0]) {
                case "create":
                    tableName = args[1];
                    fields = Arrays.copyOfRange(args, 2, args.length);
                    createTable(tableName, fields);
                    break;
                case "put":
                    tableName = args[1];
                    row = args[2];
                    fields = new String[]{args[3]};
                    values = new String[]{args[4]};
                    addRecord(tableName, row, fields, values);
                    break;
                case "scan":
                    tableName = args[1];
                    column = args[2];
                    scanColumn(tableName, column);
                    break;
                case "delete":
                    tableName = args[1];
                    row = args[2];
                    deleteRow(tableName, row);
                    break;
                default:
                    System.err.println("No matched commands");
            }
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done");
    }

    static void createTable(String tableName, String[] fields) throws IOException {
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

    static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
            Put put = new Put(row.getBytes());
            String[] cols = fields[i].split(":");
            put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            table.put(put);
        }
        table.close();
    }

    static void scanColumn(String tableName, String column) throws IOException {
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

    static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(row.getBytes());
        table.delete(delete);
        table.close();
    }
}
