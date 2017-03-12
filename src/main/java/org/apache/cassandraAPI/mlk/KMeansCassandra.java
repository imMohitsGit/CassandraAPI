package org.apache.cassandraAPI.mlk;
/**
 * Created by s152134 on 3/5/2017.
 */

import org.apache.cassandraAPI.api.SaveCassandraMLK;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

//import org.apache.flink.examples.java.clustering.util.mlk.KMeansData;

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * <p>
 * K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 *
 * <p>
 * This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 *
 * <p>
 * Usage: <code>mlk.KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {link org.apache.flink.examples.java.clustering.util.mlk.KMeansData} and 10 iterations.
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 */
@SuppressWarnings("serial")
public class KMeansCassandra {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment strenv = StreamExecutionEnvironment.getExecutionEnvironment();

       env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface
        strenv.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<KMeans.Point> points = getPointDataSet(params, env);
        DataSet<KMeans.Centroid> centroids = getCentroidDataSet(params, env);

        // set number of bulk iterations for mlk.KMeans algorithm
        IterativeDataSet<KMeans.Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<KMeans.Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new KMeans.SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // count and sum point coordinates for each centroid
                .map(new KMeans.CountAppender())
                .groupBy(0).reduce(new KMeans.CentroidAccumulator())
                // compute new centroids from point counts and coordinate sums
                .map(new KMeans.CentroidAverager());

        // feed new centroids back into next iteration
        DataSet<KMeans.Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, KMeans.Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new KMeans.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

       DataSet<Tuple3<Integer, Double, Double>> newcluster =  clusteredPoints.map(new MapFunction<Tuple2<Integer,KMeans.Point>, Tuple3<Integer, Double, Double>>() {

            public Tuple3<Integer, Double, Double> map (Tuple2<Integer, KMeans.Point> val1) {
                return  new Tuple3<Integer, Double, Double> (val1.f0,val1.f1.x, val1.f1.y);
            }

        });

        List<Tuple3<Integer, Double, Double>> Format1= newcluster.collect();
        ArrayList<Vector<Double>> Format2 = new ArrayList<Vector<Double>>();
        for(Tuple3<Integer, Double, Double> tuple : Format1) {
            Vector<Double> newVector = new Vector<Double>();

            newVector.add(tuple.f0.doubleValue());
            newVector.add(tuple.f1);
            newVector.add(tuple.f2);


            Format2.add(newVector);
        }
        // emit result
//        System.out.println(Format1);
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", ",");
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("mlk.KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            ArrayList<Tuple4<Integer,Integer,Double,Double>> collection = SaveCassandraMLK.addFunStr(Format2);
            printListT(collection);
            SaveCassandraMLK.setQuery(SaveCassandraMLK.addFunStr(Format2),strenv);
            strenv.execute("mlk.KMeans Example");
        }

    //String newpoints = clusteredPoints.toString();
    //SaveCassandraKMean.setQuery1(newpoints, env);
    }

    // *************************************************************************
    //     DATA SOURCE READING (POINTS AND CENTROIDS)
    // *************************************************************************

    private static DataSet<KMeans.Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<KMeans.Centroid> centroids;
        if (params.has("centroids")) {
            centroids = env.readCsvFile(params.get("centroids"))
                    .fieldDelimiter(" ")
                    .pojoType(KMeans.Centroid.class, "id", "x", "y");
        } else {
            System.out.println("Executing K-Means example with default centroid data set.");
            System.out.println("Use --centroids to specify file input.");
            centroids = KMeansData.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }
    public static void printList(ArrayList<Vector<Double>> list){
        for(Vector elem : list){
            System.out.println(elem+"  ");
        }
    }
    public static void printListT(ArrayList<Tuple4<Integer,Integer,Double,Double>> list){
        for(Tuple4<Integer, Integer, Double, Double> elem : list){
            System.out.println(elem+"  ");
        }
    }
    private static DataSet<KMeans.Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<KMeans.Point> points;
        if (params.has("points")) {
            // read points from CSV file
            points = env.readCsvFile(params.get("points"))
                    .fieldDelimiter(" ")
                    .pojoType(KMeans.Point.class, "x", "y");
        } else {
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = KMeansData.getDefaultPointDataSet(env);
        }
        return points;

    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    /**
     * A simple two-dimensional point.
     */
}
