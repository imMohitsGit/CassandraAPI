package org.apache.cassandraAPI.api;
/**
 * Created by s152134 on 3/5/2017.
 */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Vector;


/**
 * This is an example showing the to use the Cassandra Input-/OutputFormats in the Batch API.
 *
 * The example assumes that a table exists in a local cassandra database, according to the following query:
 * CREATE TABLE matrix1 (
 row_id int,
 column_id int,
 value varchar,
 PRIMARY KEY ( (row_id ), column_id ));
 */
public class SaveCassandra {
    /* Cassandra Query Definition*/
    private static final String INSERT_QUERY = "INSERT INTO test.matrix1 (row_id, column_id, value) VALUES (?,?,?);";
    private static final String SELECT_QUERY = "SELECT row_id, column_id, value FROM test.matrix1;";

    /*
     *	table script: "CREATE TABLE test.matrix1 (row_id int, column_id int, value varchar, PRIMARY KEY((row_id), column_id));"
     */
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
        ArrayList<Vector<String>> states5 = new ArrayList<Vector<String>>();
        while ((line = br.readLine()) != null) {
            /**
             * Reading file's each elements which are separated with comma (,), and
             * Storing the elements in the Java Tuples
             * */
            String[] entries = line.split(",");
            Vector<String> state5 = new Vector<String>(Arrays.asList(entries));
            states5.add(state5);
        }
        ArrayList<Tuple3<Integer, Integer, String>> collection1 = addFunStr(states5);
        /*Calling the function SetQuery, which accept ArrayList and executionEnvironment*/
        setQuery(collection1,streamExecutionEnvironment);
        streamExecutionEnvironment.execute("Write");
        /*Uncomment for retrieving from the table*/
        //getQuery(env);
    }
    public static DataStream<Tuple3<Integer, Integer, String>> setQuery(ArrayList<Tuple3<Integer, Integer, String>> collection, StreamExecutionEnvironment env) throws Exception {
        DataStream<Tuple3<Integer, Integer, String>> stream = env.fromCollection(collection);
        CassandraSink.addSink(stream).setQuery(INSERT_QUERY)
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    public Cluster buildCluster(Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .build();

        return stream;
    }
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
    public static ArrayList<Tuple3<Integer, Integer, String>> addFunStr(ArrayList<Vector<String>> colVector) throws Exception {
        ArrayList<Vector<String>> states5 =  colVector;
        /*Getting length of the csv file, we get total numbers of the rows in file*/
        long length = states5.size();
        // Vector<String> first = states5.get(0);
        long column_length = states5.get(0).size();
        ArrayList<Tuple3<Integer, Integer, String>> collection = new ArrayList<Tuple3<Integer, Integer, String>>();
        for (int row_id = 1; row_id <= length; row_id++) {
            for (int col_id = 1; col_id <= column_length; col_id++) {
                collection.add(new Tuple3<Integer, Integer, String>(row_id, col_id, states5.get(row_id - 1).get(col_id-1)));
            }
        }
        return collection;
    }

    public static DataSet<Tuple3<Integer, Integer, String>> getQuery(ExecutionEnvironment env) throws Exception {
        DataSet<Tuple3<Integer, Integer, String>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple3<Integer, Integer, String>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Builder builder) {
                        return builder.addContactPoints("127.0.0.1").build();
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple3<Integer, Integer, String>>() {
                }));
        return inputDS;
    }
    public static DataSet<Tuple3<Integer, Integer, String>> ReadNew(ExecutionEnvironment env) throws Exception {
        DataSet<Tuple3<Integer, Integer, String>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple3<Integer, Integer, String>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Builder builder) {
                        return builder.addContactPoints("127.0.0.1").build();
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple3<Integer, Integer, String>>() {
                }));
        return inputDS;
    }
}
