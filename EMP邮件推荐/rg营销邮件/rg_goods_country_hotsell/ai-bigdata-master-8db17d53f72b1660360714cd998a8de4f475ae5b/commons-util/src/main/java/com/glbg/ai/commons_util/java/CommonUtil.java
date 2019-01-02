package com.glbg.ai.commons_util.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;

public class CommonUtil {

    public static void main(String[] args) throws Exception {

//        System.out.println(a);
        System.out.println(org.apache.commons.codec.binary.Hex.encodeHex("中文".getBytes("UTF-8")));

        String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes("197762903_GBBR_it"));
        System.out.println(rowkey);
    }


    public static Integer getJsonValue(String key, String jsonString) {

        String value = jsonString.substring(jsonString.indexOf(",\"" + key + "\":\"") + key.length() + 5, jsonString.length() - 2);

        if (value.equals("")) return 50;
        else try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return 50;
        }
    }

    public static String buildJson(String[] columns_array, String[] data_array) {
        StringBuilder sb = new StringBuilder("{");

        for (int i = 0; i < data_array.length; i++) {
            sb.append("\"").append(columns_array[i].trim()).append("\"").append(":").append("\"").append(data_array[i].trim())
                    .append("\"").append(",");
        }
        return sb.deleteCharAt(sb.length() - 1).append("}").toString().trim().replace("\\N", "");
    }


    public static <T> String convertString(T a) {
        if (a == null) return "";
        else return String.valueOf(a);
    }


    /**
     * loading HFile in 'hFilePath' to HBase, target HTable's name is 'tableNameStr'
     *
     * @param conf         Configuration instance
     * @param hFilePath
     * @param tableNameStr
     * @throws Exception
     */
    public static void doBulkLoad(Configuration conf, String hFilePath, String tableNameStr) throws Exception {
        //change permission first.
        FileSystem fs = FileSystem.newInstance(conf);
        chmod(new Path(hFilePath), fs);
        //do bulk load.
        HBaseConfiguration.addHbaseResources(conf);
        LoadIncrementalHFiles loadFiles = new LoadIncrementalHFiles(conf);
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = connection.getTable(tableName);
        HTable hTable = new HTable(conf, tableName);
        loadFiles.doBulkLoad(new Path(hFilePath), hTable);
    }


    private static final short FULL_GRANTS = (short) 0777;

    /**
     * change the permission of a give path to 777, all subdir are changed recursively.
     *
     * @param path
     * @param fs
     * @throws IOException
     */
    public static void chmod(Path path, FileSystem fs) throws IOException {
        fs.setPermission(path, FsPermission.createImmutable(FULL_GRANTS));
        if (fs.getFileStatus(path).isFile()) {
            return;
        }
        RemoteIterator<LocatedFileStatus> fileStatuses = fs.listLocatedStatus(path);
        while (fileStatuses.hasNext()) {
            LocatedFileStatus status = fileStatuses.next();
            if (status != null) {
                fs.setPermission(status.getPath(), FsPermission.createImmutable(FULL_GRANTS));
                chmod(status.getPath(), fs);
            }
        }
    }

}
