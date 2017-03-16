package org.apache.cassandraAPI.test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
        BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Big Data\\Data\\test.csv")));
        String line;
        /* Creating ArrayList of Vector, which will store all the elements from the file*/
        ArrayList<Vector<Double>> states5 = new ArrayList<Vector<Double>>();
        int numcol = 10;
        for(int k = 1; k < 10000; k++){
            Vector<Double> vector = new Vector<Double>(numcol);
            for (int v =0 ; v<=numcol-1; v++)
            {
                vector.add(v, 0.1);
            }
            states5.add(vector);
        }
        int sizeoffile = states5.size();
        ArrayList<Tuple4<Integer, Integer,Double,Double>> collection1 = addFunStr(states5);
        /*Calling the function SetQuery, which accept ArrayList and executionEnvironment*/
        long startTime = System.currentTimeMillis();

      setQuery(collection1,streamExecutionEnvironment);
       streamExecutionEnvironment.execute("Write");
        /*Uncomment for retrieving from the table*/
        //getQuery3(env).print();
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.println("Execution time for the Set method is: " + duration +" milliseconds and size of file is " +sizeoffile*numcol*2 + " Byte" );
    }
}
