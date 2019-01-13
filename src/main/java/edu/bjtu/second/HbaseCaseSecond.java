package edu.bjtu.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class HbaseCaseSecond {

    private static final String FILENAME = "a11.txt";
    private static Configuration config = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException {
        BufferedReader br = null;
        FileReader fr = null;

        //create table
        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(config);

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf("sensors"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("data"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");

        try {

            fr = new FileReader(FILENAME);
            br = new BufferedReader(fr);

            String sCurrentLine;

            // Instantiating HTable class
            HTable hTable = new HTable(config, "sensors");

            String rowKey = "";
            String columnName = "";
            String value = "";
            Put p = null;
            int lineNum = 0;

            while ((sCurrentLine = br.readLine()) != null) {
                String[] parts = sCurrentLine.split(",");
                for (int i = 1; i < 30; i++) {
                    rowKey = parts[0];
                    columnName = "c" + i ;
                    value = parts[i];

                    System.out.println("rowKey: " + rowKey + " columnName: " + columnName + " value: " + value);
                    p = new Put(Bytes.toBytes(rowKey));
                    p.add(Bytes.toBytes("data"),
                            Bytes.toBytes(columnName), Bytes.toBytes(value));
                    hTable.put(p);
                }
                lineNum ++;
                if(lineNum == 2)
                    break;
            }
            System.out.println("data inserted");

            // closing HTable
            hTable.close();

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

            try {

                if (br != null)
                    br.close();

                if (fr != null)
                    fr.close();

            } catch (IOException ex) {

                ex.printStackTrace();

            }

        }

    }

}
