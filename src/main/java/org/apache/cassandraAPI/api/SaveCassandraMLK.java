package org.apache.cassandraAPI.api;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Vector;

/**
 * Created by s152134 on 3/12/2017.
 * This file save the state of any KMean machine learning algorithm which has cluster ID the values of coordinates
 */
public class SaveCassandraMLK {
    /* Cassandra Query Definition*/
    private static final String INSERT_QUERY = "INSERT INTO test.matrix4 (row_id, column_id, unique_id, value) VALUES (?,?,?,?);";
    private static final String SELECT_QUERY = "SELECT row_id, column_id, unique_id, value FROM test.matrix4 where row_id <=140000 allow filtering;";
    private  static  final String GET_STATE = "";

    /*
     *	table script: "CREATE TABLE matrix4 (
        row_id int,
        column_id int,
	    unique_id double,
        value double,
        PRIMARY KEY ((row_id , column_id),unique_id));"
     */
    public static void main(String[] args) throws Exception {

        /*Batch Environment creation*/
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        /*Streaming Environment creation*/
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*To read the file from stream Buffer reader is used*/
        BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Big Data\\Data\\test1.csv")));
        String line;
        /* Creating ArrayList of Vector, which will store all the elements from the file*/
        ArrayList<Vector<Double>> states5 = new ArrayList<Vector<Double>>();
        while ((line = br.readLine()) != null) {
            /**
             * Reading file's each elements which are separated with comma (,), and
             * Storing the elements in the Java Tuples
             * */
            String[] entries = line.split(",");
            Double[] entriesd = new Double[entries.length] ;

            for (int i = 0; i < entries.length; ++i) { //iterate over the elements of the list
                entriesd[i] = Double.parseDouble(entries[i]); //store each element as a double in the array
            }
            Vector<Double> state5 = new Vector<Double>(Arrays.asList(entriesd));
            states5.add(state5);
        }
        ArrayList<Tuple4<Integer, Integer,Double, Double>> collection1 = addFunStr(states5);
        /*Calling the function SetQuery, which accept ArrayList and executionEnvironment*/
        setQuery(collection1,streamExecutionEnvironment);
        streamExecutionEnvironment.execute("Write");
        /*Uncomment for retrieving from the table*/
        getQuery2(env).print();
    }
    public static DataStream<Tuple4<Integer, Integer, Double,Double>> setQuery(ArrayList<Tuple4<Integer, Integer, Double,Double>> collection, StreamExecutionEnvironment env) throws Exception {
        DataStream<Tuple4<Integer, Integer, Double,Double>> stream = env.fromCollection(collection);
        CassandraSink.addSink(stream).setQuery(INSERT_QUERY)
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    public Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build(); //Change ibm-power-1 on cluster otherwise 127.0.0.1
                    }
                })
                .build();

        return stream;
    }
    /*Function to change ArrayList of vector to ArrayList of Tuple3*/
    public static ArrayList<Tuple3<Integer, Integer, Double>> addFunDouble(ArrayList<Vector<Double>> colVector) throws Exception {
        ArrayList<Vector<Double>> states5 =  colVector;
        /*Getting length of the csv file, we get total numbers of the rows in file*/
        long length = states5.size();
        // Vector<String> first = states5.get(0);
        long column_length = states5.get(0).size();
        ArrayList<Tuple3<Integer, Integer, Double>> collection = new ArrayList<Tuple3<Integer, Integer, Double>>();
        for (int row_id = 1; row_id <= length; row_id++) {
            for (int col_id = 1; col_id <= column_length; col_id++) {

                collection.add(new Tuple3<Integer, Integer, Double>(row_id, col_id, states5.get(row_id - 1).get(col_id-1)));
            }
        }
        return collection;
    }
    /**
     * Function to change ArrayList of vector to ArrayList of Tuple4, this will be used to store the state in the cassandra format
    * Row_id, Column_id, Unique_id, Value
    * */
    public static ArrayList<Tuple4<Integer, Integer, Double,Double>> addFunStr(ArrayList<Vector<Double>> colVector) throws Exception {
        ArrayList<Vector<Double>> states5 =  colVector;
        /*Getting length of the csv file, we get total numbers of the rows in file*/
        long length = states5.size();
        // Vector<String> first = states5.get(0);
        long column_length = states5.get(0).size();
        ArrayList<Tuple4<Integer, Integer,Double, Double>> collection = new ArrayList<Tuple4<Integer, Integer, Double, Double>>();
        for (int row_id = 1; row_id <= length; row_id++) {
            for (int col_id = 1; col_id < column_length; col_id++) {
                collection.add(new Tuple4<Integer, Integer, Double, Double>(row_id, col_id, states5.get(row_id - 1).get(0), states5.get(row_id - 1).get(col_id)));
            }
        }
        return collection;
    }
/*It is used to retrieve whole row from the matrix but it is not used to retrieve the state of  Machine Learning Algorithm*/
    public static DataSet<Tuple3<Integer, Integer, String>> getQuery2(ExecutionEnvironment env) throws Exception {
        DataSet<Tuple3<Integer, Integer, String>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple3<Integer, Integer, String>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoints("ibm-power-1").build(); //Change ibm-power-1 on cluster
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple3<Integer, Integer, String>>() {
                }));
        return inputDS;
    }
    /*It is used to retrieve whole row from the matrix and it is used to retrieve the state of  Machine Learning Algorithm */
    public static DataSet<Tuple4<Integer, Integer, Double, Double>> getQuery(ExecutionEnvironment env) throws Exception {
        DataSet<Tuple4<Integer, Integer, Double, Double>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple4<Integer, Integer, Double, Double>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoints("127.0.0.1").build();
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple4<Integer, Integer, Double, Double>>() {
                }));
        return inputDS;
    }
    /*It is used to retrieve whole row from the matrix and it is used to retrieve the state of  Machine Learning Algorithm */
    public static DataSet<Tuple4<Integer, Integer, Double, Double>> getQuery3(ExecutionEnvironment env) throws Exception {
        DataSet<Tuple4<Integer, Integer, Double, Double>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple4<Integer, Integer, Double, Double>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoints("127.0.0.1").build();
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple4<Integer, Integer, Double, Double>>() {
                }));
        return inputDS;
    }
}
