import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @description:
 * @author: cuibowen
 * @create: 2019/01/22
 **/
public class CanalKafkaStreaming {

    public static void main(String[] args) throws InterruptedException {

        //按照CPU核心数执行
        SparkConf conf = new SparkConf()
                .setAppName("Spark streaming test")
                .setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(20000));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Collections.singletonList("example");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        //stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        //stream.map(record -> record.value()).print();
        stream.map(record -> record.value())
                .map(new ObjectMapper()::readTree)
                .mapToPair(jsonNode -> new Tuple2<>(jsonNode.get("type").asText(), jsonNode.get("data").size()))
                .reduceByKey(Integer::sum)
                .print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
