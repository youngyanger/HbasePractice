package edu.bjtu.first;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseReadFirst {

    //declare initial value
    static double[][] sim = new double[29][29];//final result
    static double[] sum = new double[29];//array sum
    static double[] sum_sq = new double[29];//array pow2 sum
    static double[][] p_sum = new double[29][29];//product sum
    static int common_items_len = 0; //match number
    static ArrayList<String>[] list = new ArrayList[29];

    public static void main(String[] args) throws IOException{

        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        // Instantiating HTable class
        HTable table = new HTable(config, "sensor");

        Scan scan = new Scan();
        // get the ResultScanner
        ResultScanner scanner = table.getScanner(scan);

        //Assign value to the initial value
        for(Result r: scanner){
            String str = new String(r.getRow());
            String[] rowKey = str.split(":");
            int rowKeyNum = Integer.valueOf(rowKey[0].substring(1));
            rowKeyNum--;
            if(list[rowKeyNum] == null){
                list[rowKeyNum] = new ArrayList<String>();
            }
            if(!list[rowKeyNum].contains(str)){
                list[rowKeyNum].add(str);
            }

            for(KeyValue kv:r.raw()){
                //System.out.println("  value=>"+new String(kv.getValue(),"utf-8"));
                String value = new String(kv.getValue(),"utf-8");
                double doubleValue = Double.parseDouble(value);
                sum[rowKeyNum] += doubleValue;
                sum_sq[rowKeyNum] += Math.pow(doubleValue,2);
            }
        }

        //print final sim result through loop
        for(int i = 0; i < 29; i++){
            for(int j = 0; j < 29; j++){
                System.out.print(calculateSim(table,i,j) + "\t");
            }
            System.out.println("");
        }

        //close scanner
        scanner.close();
    }

    public static double calculateSim(HTable table, int i1, int i2) throws IOException {
        //calculate i1 and i2 's product sum
        ArrayList resultList = null;
        for(int i = 0; i < list[i1].size(); i++){

            Get g1 = new Get(Bytes.toBytes(list[i1].get(i)));
            Get g2 = new Get(Bytes.toBytes(list[i2].get(i)));

            Result result1 = table.get(g1);
            Result result2 = table.get(g2);

            resultList = new ArrayList<Double>();
            for(KeyValue kv:result2.raw()){
                String value = new String(kv.getValue(),"utf-8");
                Double doubleValue = Double.parseDouble(value);
                resultList.add(doubleValue);
            }
            for(KeyValue kv:result1.raw()){
                String value = new String(kv.getValue(),"utf-8");
                Double doubleValue = Double.parseDouble(value);
                Double anotherValue = (Double) resultList.get(common_items_len);
                p_sum[i1][i2] += doubleValue*anotherValue;
                common_items_len++;
            }

        }

        //calculate sim
        double num = common_items_len * p_sum[i1][i2] - sum[i1] * sum[i2];
        double den = Math.sqrt((common_items_len * sum_sq[i1] - Math.pow(sum[i1], 2))
                * (common_items_len * sum_sq[i2] - Math.pow(sum[i2], 2)));
        common_items_len = 0;
        return sim[i1][i2] = (den == 0) ? 1 : num / den;
    }
}
