package org.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MultiOpt {

    static Admin admin = null;
    static Connection connection = null;

    public static void main(String[] args) {

    }

    public static boolean tableExist(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    void createTable(String tableName, String[] fields) throws IOException {
        // 判断是否存在列族信息
        if (fields.length == 0) {
            System.out.println("请设置列族信息！");
            return;
        }
        //判断表是否存在
        if (tableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
        } else {
            //创建表描述器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : fields) {
                // 创建列族描述器
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            //根据对表的配置，创建表
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {

    }

    void scanColumn(String tableName, String column) {

    }

    void modifyData(String tableName, String row, String column) {

    }

    void deleteRow(String tableName, String row) {

    }
}
