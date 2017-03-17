package org.apache.cassandraAPI.test;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.WriteFormatAsCsv;

import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

import static org.apache.cassandraAPI.api.SaveCassandraMLK.*;

/**
 * Created by s152134 on 3/15/2017.
 */
public class testing {
    public static void main(String[] args) throws Exception {

        /*Batch Environment creation*/
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /*Streaming Environment creation*/
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*To read the file from stream Buffer reader is used*/
       // BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Big Data\\Data\\test.csv")));
        String line;
        /* Creating ArrayList of Vector, which will store all the elements from the file*/
        ArrayList<Vector<Double>> states5 = new ArrayList<Vector<Double>>();
        Integer numrow = Integer.parseInt(args[0]);
        Integer numcol = Integer.parseInt(args[1]);

        for(int k = 1; k <= numrow; k++){
            Vector<Double> vector = new Vector<Double>(numcol);
            for (int v =0 ; v<=numcol-1; v++)
            {
                vector.add(v, 0.1);
            }
            states5.add(vector);
        }
        Integer sizeoffile = states5.size();
        //DataSet<Tuple4<Integer, Integer, Double, Double>> elements = null;

        Double time = 0.0;
        Double[] duration = new Double[10];
        Double[] std = new Double[10];
        Integer experiment =Integer.parseInt(args[2]);
        Double st_dev = 0.0;
        ArrayList<Tuple4<Integer, Integer,Double,Double>> collection1 = addFunStr(states5);
        /*Calling the function SetQuery, which accept ArrayList and executionEnvironment*/
        for(int exp =0 ; exp <experiment ;exp++) {
            long startTime = System.currentTimeMillis();

 /*Save into Cassandra using DataSet*/
//        setQueryDataSet(collection1,env);
//           env.execute("Write");

            /*Save into Cassandra using Cassandra Sink Streaming*/
        setQuery(collection1,streamExecutionEnvironment);
           streamExecutionEnvironment.execute("Write");
        /*Uncomment for retrieving from the table*/
//            getQuery3(env, args[0]).print();

            long endTime = System.currentTimeMillis();
            duration[exp] = Double.valueOf((endTime - startTime));  //divide by 1000000 to get milliseconds.
            time = time+duration[exp];
            //System.out.println("Execution time for the Set method is: " + duration + " milliseconds and size of file is " + sizeoffile * numcol * 2 + " Byte");
        }
        Double meantime = time/experiment;
        for (int ex=0; ex<experiment;ex++){
            std[ex] = Math.pow((duration[ex] -  (meantime)),2);
            st_dev = st_dev + std[ex];
        }
        Double standardDev = Math.sqrt(st_dev)/experiment;
        System.out.println("Average execution time for the method is: " + meantime +" milliseconds and Standard Deviation is " + standardDev +"  " + sizeoffile * numcol * 2 + " Byte");
        Integer memorySize = sizeoffile * numcol * 2;
//        ArrayList<Tuple4<Double, Double,Integer, Integer>> datatoCSV= new ArrayList<Tuple4<Double, Double,Integer, Integer>>();
//        datatoCSV.add(new Tuple4<Double, Double,Integer, Integer> (meantime, standardDev,memorySize ,experiment));

        //Open File
        BufferedWriter writer = null;
        try {
            //create a temporary file
//            String timeLog = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
            File logFile = new File("MyFile");

            // This will output the full path where the file will be written to...
//            System.out.println(logFile.getCanonicalPath());

            writer = new BufferedWriter(new FileWriter(logFile));
            writer.write(Integer.toString(memorySize)+","+Integer.toString(experiment)+","+Integer.toString(numrow)+","+Integer.toString(numcol)+","+Double.toString(meantime)+","+Double.toString(standardDev)+"\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                // Close the writer regardless of what happens...
                writer.close();
            } catch (Exception e) {
            }
        }



    }
}
