package movieAnalysis.rdd.secondsort.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * @Description 说明类的用途
 * @ClassName SecondSortingTest
 * @Author zzd
 * @Create 2019/9/12 14:33
 * @Version 1.0
 **/
public class MovieSecondSorting {
    public static void main(String[] args) {
        String path = "D:\\scalaWorkingSpace\\data\\ml-1m\\ratings.dat";
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("SecondSortingTest"));
        JavaRDD<String> lines = sc.textFile(path);
//        组装RDD
        JavaPairRDD<SecondarySortingKey, String> keyvalues = lines
                .mapToPair(new PairFunction<String, SecondarySortingKey, String>() {
                    @Override
                    public Tuple2<SecondarySortingKey, String> call(String line) throws Exception {
                        String[] splited = line.split("::");
//                        时间戳和评分作为排序依据
                        SecondarySortingKey secondarySortingKey = new SecondarySortingKey(Integer.valueOf(splited[3]), Integer.valueOf(splited[2]));
                        return new Tuple2<>(secondarySortingKey, line);
                    }
                });
//        按照key值二次排序
        JavaPairRDD<SecondarySortingKey, String> sorted = keyvalues.sortByKey(false);
        JavaRDD<String> result = sorted.map(new Function<Tuple2<SecondarySortingKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortingKey, String> tuple) throws Exception {
//                取第二值
                return tuple._2;
            }
        });
        List<String> collected = result.take(10);
        for (String item : collected) {
            System.out.println(item);
        }
    }
}
