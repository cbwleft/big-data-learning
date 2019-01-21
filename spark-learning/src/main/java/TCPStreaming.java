import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @description:
 * @author: cuibowen
 * @create: 2019/01/21
 **/
public class TCPStreaming {

    public static void main(String[] args) throws InterruptedException {
        //按照CPU核心数执行
        SparkConf conf = new SparkConf()
                .setAppName("Spark streaming test")
                .setMaster("local[*]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(1000));

        //nc -lk 9999启动TCP
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();

        jsc.start();              // Start the computation
        jsc.awaitTermination();   // Wait for the computation to terminate

    }

}
