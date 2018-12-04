import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RDDTest {

    private static SparkConf conf;
    private static JavaSparkContext sc;

    @BeforeClass
    public static void beforeClass() {
        conf = new SparkConf()
                .setAppName("RDDTest")
                .setMaster("local[*]");//按照CPU核心数执行
        sc = new JavaSparkContext(conf);
    }

    @Test
    public void testMap() {
        List<String> data = Arrays.asList("1", "2", "3", "4");
        JavaRDD<String> stringRDD = sc.parallelize(data);
        JavaRDD<Integer> intRDD = stringRDD.map(s -> Integer.parseInt(s));
        Assert.assertTrue(intRDD.first() == 1);
        Assert.assertTrue(intRDD.count() == 4);
        List<Integer> list = intRDD.collect();
        Assert.assertTrue(list.get(2) == 3);
    }

    @Test
    public void testFloatMap() {
        String describe = "Similar to map but each input item can be mapped to 0 or more output items";
        JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList(describe));
        stringRDD = stringRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        Assert.assertEquals(stringRDD.first(), "Similar");
        Assert.assertTrue(stringRDD.count() == 16);
        List<String> list = stringRDD.collect();
        Assert.assertTrue(list.contains("0"));
    }

    @Test
    public void testFilter() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> intRDD = sc.parallelize(data);
        intRDD = intRDD.filter(i -> i % 2 == 0);
        Assert.assertTrue(intRDD.first() == 2);
        Assert.assertTrue(intRDD.count() == 2);
    }

    @Test
    public void testPair() {
        JavaRDD<String> lines = sc.textFile("src/main/resources/data.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Iterable<Integer>> group = pairs.groupByKey();//将相同key聚集在一起形成Iterable
        Map<String, Long> countByKey = group.countByKey();//统计key个数
        Assert.assertTrue(countByKey.get("map") == 1);

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);//将相同key聚集在一起并规约
        Map<String, Integer> collect = counts.collectAsMap();
        Assert.assertTrue(collect.get("map") == 2);
    }

    @Test
    public void testReduce() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> intRDD = sc.parallelize(data);
        int multi = intRDD.reduce((a, b) -> a * b);
        Assert.assertTrue(multi == 120 );
        int sum = intRDD.reduce((a, b) -> a + b);
        Assert.assertTrue( sum == 15);
    }
}
