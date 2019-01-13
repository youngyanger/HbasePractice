package edu.bjtu.second;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseReadSecond {

    //declare initial value
    static double[][] sim = new double[29][29];//final result
    static double[] sum = new double[29];//array sum
    static double[] sum_sq = new double[29];//array pow2 sum
    static double[][] p_sum = new double[29][29];//product sum
    static int common_items_len = 0; //match number

    public static void main(String[] args) throws IOException{

        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "sensors");

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        //Assign value to the initial value
        int i,j;
        for(Result r: scanner){

            Set<Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> entries = r.getMap().entrySet();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry: entries) {

                for(Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry: familyEntry.getValue().entrySet()) {
                    byte[] column = columnEntry.getKey();

                    if (columnEntry.getValue().size()>0) {

                        Map.Entry<Long, byte[]> valueEntry = columnEntry.getValue().firstEntry();
                        double doubleValue = Double.parseDouble(Bytes.toStringBinary(valueEntry.getValue()));
                        i = Integer.parseInt(Bytes.toStringBinary(column).substring(1)) - 1;
                        sum[i] += doubleValue;
                        sum_sq[i] += doubleValue*doubleValue;

                        for(Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntrys: familyEntry.getValue().entrySet()) {

                            byte[] columns = columnEntrys.getKey();
                            Map.Entry<Long, byte[]> valueEntrys = columnEntrys.getValue().firstEntry();
                            double doubleValues = Double.parseDouble(Bytes.toStringBinary(valueEntrys.getValue()));
                            j = Integer.parseInt(Bytes.toStringBinary(columns).substring(1)) - 1;
                            p_sum[i][j] += doubleValue*doubleValues;

                        }

                    }
                }
            }
            common_items_len++ ;
        }

        //print final sim result through loop
        for(int k = 0; k < 29; k++){
            for(int l = 0; l < 29; l++){
                System.out.print(calculateSim(k,l) + "\t");
            }
            System.out.println("");
        }

        //close scanner
        scanner.close();
    }

    public static double calculateSim(int i1, int i2){

        //calculate sim
        double num = common_items_len * p_sum[i1][i2] - sum[i1] * sum[i2];
        double den = Math.sqrt((common_items_len * sum_sq[i1] - Math.pow(sum[i1], 2))
                * (common_items_len * sum_sq[i2] - Math.pow(sum[i2], 2)));
        return sim[i1][i2] = (den == 0) ? 1 : num / den;

    }

}
