package ebrealtimestreamprocessing.java;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @Description 说明类的用途
 * @ClassName AdClickedStreamingStats
 * @Author zzd
 * @Date 2019/10/9 11:08
 * @Version 1.0
 **/
public class AdClickedStreamingStats {

    public static void main(String[] args) {

        /**
         * 第一步：
         * d首先配置SparkConfig
         */
//    SparkConf conf=new SparkConf().setMaster("localhost:7077").setAppName("AdClickedStreamingStats");
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("AdClickedStreamingStats")
                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.ui.port","4040");
        /**
         * 第二步：
         * 创建SparkSteamContext
         */
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        jsc.checkpoint("D:\\scalaWorkingSpace\\data\\checkpoint");
        /**
         * 第三步
         * 创建SparkStreaming输入数据来源
         */
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");//多个可用ip可用","隔开
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("group.id", "sparkStreaming");
        Set<String> topics = new HashSet<>();
        topics.add(Constants.KAFKA_TOPIC);

        JavaInputDStream<ConsumerRecord<Object, Object>> adClickedStreaming = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        /**
         *黑名单过滤
         */
        JavaPairDStream<String, String> filteredAdClickedStreaming = adClickedStreaming.transformToPair(
                new Function<JavaRDD<ConsumerRecord<Object, Object>>, JavaPairRDD<String, String>>() {
            @Override
            public JavaPairRDD<String, String> call(JavaRDD<ConsumerRecord<Object, Object>> rdd) throws Exception {
                /**
                 * 黑名单过滤
                 * 1.从数据库中获取黑名单并转换成rdd，
                 * 2.将代表黑名单的RDD实例和batchDuration 拿到的rdd进行join【leftouterjoin】
                 * 如果两边都有内容则为true，否则为false，我们要false的【不在黑名单里面的】
                 */

                final List<String> blackList = new ArrayList<>();
                JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
                jdbcWrapper.doQuery("select * from t_black_list", null, new ExecuteCallBack() {
                    @Override
                    public void resultCallBack(ResultSet resultSet) throws SQLException {
                        while (resultSet.next()) {
                            blackList.add(resultSet.getString(1));
                        }
                    }
                });
                List<Tuple2<String, Boolean>> blackListTuple = new ArrayList<>();
                for (String name : blackList) {
                    blackListTuple.add(new Tuple2<>(name, true));
                }

                List<Tuple2<String, Boolean>> blackListFromDb = blackListTuple;
                JavaSparkContext jsc = new JavaSparkContext(rdd.context());
                /**
                 * 黑名单的表中只有userid，但是如果要进行join，必须是key-value，所以需要将数据表中的数据转换为key-value集合
                 */
                JavaPairRDD<String, Boolean> blackListRDD = jsc.parallelizePairs(blackListFromDb);
                /**
                 * AD的数据基本格式：timestamp,ip,userId,adId,province,city
                 */
                JavaPairRDD<String, Tuple2<String, String>> rdd2Pair = rdd.mapToPair(new PairFunction<ConsumerRecord<Object, Object>, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, String>> call(ConsumerRecord<Object, Object> consumerRecord) throws Exception {
                        String userId = consumerRecord.value().toString().split("\t")[2];
                        System.out.println(consumerRecord.value().toString());
                        System.out.println("consumerRecord.key()"+consumerRecord.key());
                        Tuple2<String, String> tuple = new Tuple2<>(consumerRecord.key().toString(), consumerRecord.value().toString());
                        return new Tuple2<String, Tuple2<String, String>>(userId, tuple);
                    }
                });

                JavaPairRDD<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joined = rdd2Pair.leftOuterJoin(blackListRDD);
                JavaPairRDD<String, String> result = joined.filter(new Function<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> v1) throws Exception {
                        Optional<Boolean> optional = v1._2._2;
                        if (optional.isPresent() && optional.get()) {
                            //在黑名单内的return false,过滤掉
                            return false;
                        } else {
                            return true;
                        }
                    }
                }).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Tuple2<Tuple2<String, String>, Optional<Boolean>>> t) throws Exception {
                        //从过滤后的RDD中拿出有效tuple
                        return t._2._1;
                    }
                });
                return result;
            }
        });

        filteredAdClickedStreaming.print();

        /**
         * 第四步：
         * 基于DSStream进行编程
         *
         * AD点击的基本数据格式：timestamp,ip,userId,adId,province,city
         */
        JavaPairDStream<String, Long> pairs = filteredAdClickedStreaming.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] splited = t._2.split("\t");
                String timestamp = splited[0];
                String ip = splited[1];
                String userId = splited[2];
                String adId = splited[3];
                String province = splited[4];
                String city = splited[5];
                String clickedRecord = timestamp + "_" + ip + "_" + userId + "_" + adId + "_" + province + "_" + city;
                return new Tuple2<>(clickedRecord, 1L);
            }
        });
        /**
         * 计算每个batchDuration中每个user的AD点击量
         */
        JavaPairDStream<String, Long> adClickedUsers = pairs.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * 计算有效点击：避免刷数据，标准为一天内一个用户点击AD的次数过多【超过50次】就加入黑名单
         *
         * 黑名单需要动态生成，除了数据库中已经有的，对于新的黑名单要保存，要添加
         */
        JavaPairDStream<String, Long> filteredClickInBatch = adClickedUsers.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> v1) throws Exception {
//                大于N次就放入黑名单，TODO，测试用
                if (5 < v1._2) {
                    return false;
                } else {
                    return true;
                }
            }
        });
        filteredClickInBatch.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                {
                    if (rdd.isEmpty()) {
                        return;
                    }
                    rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                        @Override
                        public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                            List<UserAdClicked> userAdClickedList = new ArrayList<>();
                            while (partition.hasNext()) {
                                Tuple2<String, Long> record = partition.next();
                                String[] splited = record._1.split("_");
                                UserAdClicked userAdClicked = new UserAdClicked();
                                userAdClicked.setTimestamp(splited[0]);
                                userAdClicked.setIp(splited[1]);
                                userAdClicked.setUserId(splited[2]);
                                userAdClicked.setAdId(splited[3]);
                                userAdClicked.setProvince(splited[4]);
                                userAdClicked.setCity(splited[5]);
                                userAdClickedList.add(userAdClicked);
                            }
                            final List<UserAdClicked> inserting = new ArrayList<>();
                            final List<UserAdClicked> updating = new ArrayList<>();
                            JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
                            //点击表的字段：timestamp,ip,userId,adId,province,city,clickedCount
                            for (final UserAdClicked clicked : userAdClickedList) {
                                jdbcWrapper.doQuery("select count(1) from t_adclicked where time_stamp = ?" +
                                        "and user_id=? and ad_id=?", new Object[]{clicked.getTimestamp(), clicked.getUserId(), clicked.getAdId()}, new ExecuteCallBack() {
                                    @Override
                                    public void resultCallBack(ResultSet resultSet) throws SQLException {
                                        if (resultSet.getRow() != 0) {
                                            long count = resultSet.getLong(1);
                                            clicked.setClickedCount(count);
                                            updating.add(clicked);
                                        } else {
                                            clicked.setClickedCount(0L);
                                            inserting.add(clicked);
                                        }
                                    }
                                });
                            }
//                            插入
                            ArrayList<Object[]> insertParamList = new ArrayList<>();
                            for (UserAdClicked insertRecord : inserting) {
                                insertParamList.add(new Object[]{
                                        insertRecord.getTimestamp(),
                                        insertRecord.getIp(),
                                        insertRecord.getUserId(),
                                        insertRecord.getAdId(),
                                        insertRecord.getProvince(),
                                        insertRecord.getCity(),
                                        insertRecord.getClickedCount()
                                });
                            }
                            jdbcWrapper.doBatch("insert into t_adclicked values(?,?,?,?,?,?,?)", insertParamList);
//                            更新
                            ArrayList<Object[]> updateParamList = new ArrayList<>();
                            for (UserAdClicked insertRecord : updating) {
                                updateParamList.add(new Object[]{
                                        insertRecord.getTimestamp(),
                                        insertRecord.getIp(),
                                        insertRecord.getUserId(),
                                        insertRecord.getAdId(),
                                        insertRecord.getProvince(),
                                        insertRecord.getCity(),
                                        insertRecord.getClickedCount()
                                });
                            }
                            jdbcWrapper.doBatch("update t_adclicked set clicked_count=? where time_stamp =? and " +
                                    "ip=? and user_id=? and ad_id=? and province=? and city =?", updateParamList);

                        }
                    });
                }
            }
        });
//        filteredClickInBatch.foreachRDD(new Function<JavaPairRDD<String, Long>, Void>() {
//            @Override
//            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
//                if (rdd.isEmpty()) {
//
//                }
//                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
//                        List<UserAdClicked> userAdClickedList = new ArrayList<>();
//                        while (partition.hasNext()) {
//                            Tuple2<String, Long> record = partition.next();
//                            String[] splited = record._1.split("_");
//                            UserAdClicked userAdClicked = new UserAdClicked();
//                            userAdClicked.setTimestamp(splited[0]);
//                            userAdClicked.setIp(splited[1]);
//                            userAdClicked.setUserId(splited[2]);
//                            userAdClicked.setAdId(splited[3]);
//                            userAdClicked.setProvince(splited[4]);
//                            userAdClicked.setCity(splited[5]);
//                            userAdClickedList.add(userAdClicked);
//                        }
//                        final List<UserAdClicked> inserting = new ArrayList<>();
//                        final List<UserAdClicked> updating = new ArrayList<>();
//                        JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
//                        //点击表的字段：timestamp,ip,userId,adId,province,city,clickedCount
//                        for (final UserAdClicked clicked : userAdClickedList) {
//                            jdbcWrapper.doQuery("select count(1) from t_adclicked where timestamp = ?" +
//                                    "and user_id=? and ad_id=?", new Object[]{clicked.getTimestamp(), clicked.getUserId(), clicked.getAdId()}, new ExecuteCallBack() {
//                                @Override
//                                public void resultCallBack(ResultSet resultSet) throws SQLException {
//                                    if (resultSet.getRow() != 0) {
//                                        long count = resultSet.getLong(1);
//                                        clicked.setClickedCount(count);
//                                        updating.add(clicked);
//                                    } else {
//                                        clicked.setClickedCount(0L);
//                                        inserting.add(clicked);
//                                    }
//                                }
//                            });
//                        }
////                            插入
//                        ArrayList<Object[]> insertParamList = new ArrayList<>();
//                        for (UserAdClicked insertRecord : inserting) {
//                            insertParamList.add(new Object[]{
//                                    insertRecord.getTimestamp(),
//                                    insertRecord.getIp(),
//                                    insertRecord.getUserId(),
//                                    insertRecord.getAdId(),
//                                    insertRecord.getProvince(),
//                                    insertRecord.getCity(),
//                                    insertRecord.getClickedCount()
//                            });
//                        }
//                        jdbcWrapper.doBatch("insert into t_adclicked values(?,?,?,?,?,?,?)", insertParamList);
////                            更新
//                        ArrayList<Object[]> updateParamList = new ArrayList<>();
//                        for (UserAdClicked insertRecord : updating) {
//                            updateParamList.add(new Object[]{
//                                    insertRecord.getTimestamp(),
//                                    insertRecord.getIp(),
//                                    insertRecord.getUserId(),
//                                    insertRecord.getAdId(),
//                                    insertRecord.getProvince(),
//                                    insertRecord.getCity(),
//                                    insertRecord.getClickedCount()
//                            });
//                        }
//                        jdbcWrapper.doBatch("update t_adclicked set clicked_count=? where timestamp =? and " +
//                                "ip=? and user_id=? and ad_id=? and province=? and city =?", updateParamList);
//
//                    }
//                });
//                return null;
//            }
//        });

        JavaPairDStream<String, Long> blackListBasedOnHistory = filteredClickInBatch.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> v1) throws Exception {
//点击表的字段：timestamp,ip,userId,adId,province,city,clickedCount
                String[] splited = v1._1.split("_");
                String date = splited[0];
                String userId = splited[2];
                String adId = splited[3];
                /**
                 * 基于date，userId，adId查询点击数据表，获得总的点击数据
                 * 同时判断是否属于黑名单
                 */
//              TODO 查询数据库，统计今天的统计次数，这里默认全部都是81次，超过阈值50，即都是黑名单
                int clickedCountTotalToday = 8;
                if (clickedCountTotalToday > 50) {
                    return true;
                } else {
                    return false;
                }
            }
        });
/**
 * 对黑名单的RDD去重
 */
        JavaDStream<String> blackListUserIdBasedOnHistory = blackListBasedOnHistory.map(new Function<Tuple2<String, Long>, String>() {
            @Override
            public String call(Tuple2<String, Long> v1) throws Exception {
                return v1._1.split("_")[2];//userid
            }
        });
        /**
         * 获取到去重后的数据集
         */
        JavaDStream<String> blackListUniqueUserIdBasedOnHistory = blackListUserIdBasedOnHistory.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                return rdd.distinct();
            }
        });

        /**
         * 黑名单数据入库
         */
        blackListUniqueUserIdBasedOnHistory.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                if (rdd.isEmpty()) {
                    return;
                }
                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> t) throws Exception {
                        List<Object[]> blackList = new ArrayList<>();
                        while (t.hasNext()) {
                            blackList.add(new Object[]{t.next()});
                        }
                        JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
                        jdbcWrapper.doBatch("insert into t_black_list values (?)", blackList);
                    }
                });
            }
        });

        /**
         * AD点击的动态更新
         * 然后持久化到数据库中
         */
        JavaPairDStream<String, Long> updateStateByKeyDStream = filteredAdClickedStreaming.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            /**
             * 将记录格式化为key-value,key为记录，value为次数
             */
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] splited = t._2.split("\t");
                String timestamp = splited[0];
                String ip = splited[1];
                String userId = splited[2];
                String adId = splited[3];
                String province = splited[4];
                String city = splited[5];
                String clickedRecord = timestamp + "_" + ip + "_" + userId + "_" + adId + "_" + province + "_" + city;
                return new Tuple2<>(clickedRecord, 1L);
            }
        }).updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
            @Override
            /**
             * 目的：统计结果，将这批数据跟历史数据相加
             * v1:代表当前key在当前的batch Duration 中出现的集合，如{1,1,1,1,1}
             * v2:代表当前key在以前batchDuration中积累下来的结果
             *
             */
            public Optional<Long> call(List<Long> v1, Optional<Long> v2) throws Exception {
                Long clickedTotalHistory = 0L;
                if (v2.isPresent()) {
                    clickedTotalHistory = v2.get();
                }
                for (Long count : v1) {
                    clickedTotalHistory += count;
                }
                return Optional.of(clickedTotalHistory);
            }
        });

        updateStateByKeyDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                if (rdd.isEmpty()) {
                    return;
                }
                /**
                 * 更新入库 timestamp + "_" + ip + "_" + userId + "_" + adId + "_" + province + "_" + city;
                 */
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> t) throws Exception {
                        List<AdClicked> adClickedList = new ArrayList<>();
                        while (t.hasNext()) {
                            Tuple2<String, Long> recored = t.next();
                            String[] splited = recored._1.split("_");
                            System.out.println(splited);
                            AdClicked adClicked = new AdClicked();
                            adClicked.setTimestamp(splited[0]);
                            adClicked.setAdId(splited[3]);
                            adClicked.setProvince(splited[4]);
                            adClicked.setCity(splited[5]);
                            adClicked.setClickedCount(recored._2);
                            adClickedList.add(adClicked);
                        }
                        /**
                         * 有则更新，无则插入
                         */
                        JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
                        final List<AdClicked> inserting = new ArrayList<>();
                        final List<AdClicked> updating = new ArrayList<>();
                        for (final AdClicked adClicked : adClickedList) {
                            jdbcWrapper.doQuery("select count(1) from t_adclicked_count where" +
                                            "time_stamp=? and ad_id=? and province=? and city=?",
                                    new Object[]{adClicked.getTimestamp(), adClicked.getAdId(), adClicked.getProvince(),
                                            adClicked.getCity()}, new ExecuteCallBack() {
                                        @Override
                                        public void resultCallBack(ResultSet resultSet) throws SQLException {
                                            if (resultSet.getRow() != 0) {
                                                long count = resultSet.getLong(1);
                                                adClicked.setClickedCount(count);
                                                updating.add(adClicked);
                                            } else {
                                                inserting.add(adClicked);
                                            }
                                        }
                                    });
                        }
                        /**
                         * 数据库更新或者插入
                         */
                        ArrayList<Object[]> insertParamList = new ArrayList<>();
                        for (AdClicked insertRecord : inserting) {
                            insertParamList.add(new Object[]{
                                    insertRecord.getTimestamp(),
                                    insertRecord.getAdId(),
                                    insertRecord.getProvince(),
                                    insertRecord.getCity(),
                                    insertRecord.getClickedCount()
                            });
                        }
                        jdbcWrapper.doBatch("insert into t_adclicked_count values(?,?,?,?,?)", insertParamList);
                        ArrayList<Object[]> updateParamList = new ArrayList<>();
                        for (AdClicked updateRecord : updating) {
                            updateParamList.add(new Object[]{
                                    updateRecord.getTimestamp(),
                                    updateRecord.getAdId(),
                                    updateRecord.getProvince(),
                                    updateRecord.getCity(),
                                    updateRecord.getClickedCount()
                            });
                        }
                        jdbcWrapper.doBatch("update t_adclicked_count set clicked_count = ? where " +
                                "time_stamp =? and ad_id=? and province=? and city=?", updateParamList);
                    }
                });
            }
        });


        /**
         * 计算topN,计算每天各省份topN排名的AD，
         * 由于直接对RDD操作，故使用transform算子
         */
        updateStateByKeyDStream.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
            @Override
            public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                JavaRDD<Row> rowRDD = rdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
                        String[] splited = t._1.split("_");
                        String timestamp = splited[0];
//TODO                        String timestamp="2019-10-10";
                        String adId = splited[1];
                        String province = splited[2];
                        String clickedRecord = timestamp + "_" + adId + "_" + province;
                        return new Tuple2<>(clickedRecord, t._2);
                    }
                }).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }).map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> v1) throws Exception {

                        String[] splited = v1._1.split("_");
                        String timestamp = splited[0];
//TODO                        String timestamp="2019-10-10";
                        String adId = splited[1];
                        String province = splited[2];
                        return RowFactory.create(timestamp, adId, province, v1._2);
                    }
                });

                StructType structType = DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("timestamp", DataTypes.StringType, true),
                        DataTypes.createStructField("adId", DataTypes.StringType, true),
                        DataTypes.createStructField("province", DataTypes.StringType, true),
                        DataTypes.createStructField("clickedCount", DataTypes.StringType, true)
                ));

                /**
                 * hive数据仓库处理
                 * TODO
                 */
//                HiveContext 已经过时，换成SparkContext
//                SparkConf conf2 = new SparkConf().setMaster("local[*]")
//                        .setAppName("AdClickedStreamingStats")
//                        .set("spark.driver.allowMultipleContexts","true")
//                        .set("spark.ui.port","5050");
//                JavaSparkContext context = new JavaSparkContext(conf2);
                SQLContext sqlContext = SparkSession.builder().master("local").appName("hive").getOrCreate().sqlContext();
                Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRDD, structType);
                dataFrame.registerTempTable("topNTableSource");
                String IMFsqlText = "select timestamp ,adId,province,clickedCount from (" +
                        "select timestamp ,adId,province,clickedCount, row_number() over " +
                        "(PARTITION BY province ORDER BY  clickedCount DESC) rank  from topNTableSource) subquery " +
                        "where rank <=5";
                Dataset<Row> result = sqlContext.sql(IMFsqlText);
                return result.toJavaRDD();
            }
        }).foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rdd) throws Exception {
                if (rdd.isEmpty()) {
                    return;
                }
                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> t) throws Exception {
                        List<AdProvinceTopN> adProvinceTopNList = new ArrayList<>();
                        while (t.hasNext()) {
                            Row row = t.next();
                            AdProvinceTopN adProvinceTopN = new AdProvinceTopN();
                            adProvinceTopN.setTimestamp(row.getString(0));
                            adProvinceTopN.setAdId(row.getString(1));
                            adProvinceTopN.setProvince(row.getString(2));
                            adProvinceTopN.setClickedCount(row.getLong(3));
                            adProvinceTopNList.add(adProvinceTopN);
                        }

                        JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
                        Set<String> set = new HashSet<>();
                        for (AdProvinceTopN item : adProvinceTopNList) {
                            set.add(item.getTimestamp() + " " + item.getProvince());
                        }
                        ArrayList<Object[]> deleteParamList = new ArrayList<>();
                        for (String deleteRecord : set) {
                            String[] splited = deleteRecord.split("_");
                            deleteParamList.add(new Object[]{splited[0], splited[1]});
                        }
                        jdbcWrapper.doBatch("delect from t_ad_province_topn where time_stamp =? and province = ?", deleteParamList);

                        /**
                         * 计算每个省份topN排名
                         * 字段：timestamp,adId,province,clickedCount
                         */
                        ArrayList<Object[]> insertParamList = new ArrayList<>();
                        for (AdProvinceTopN record : adProvinceTopNList) {
                            insertParamList.add(new Object[]{
                                    record.getTimestamp(),
                                    record.getAdId(),
                                    record.getProvince(),
                                    record.getClickedCount()
                            });
                        }
                        jdbcWrapper.doBatch("insert into t_ad_province_topn values(?,?,?,?)", insertParamList);
                    }
                });
            }
        });

        /**
         * 计算过去半个小时内，AD的点击趋势
         * 信息包含：timestamp,ip,userId,adId,province,city
         */
        filteredAdClickedStreaming.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> t) throws Exception {
                String[] splited = t._2.split("\t");
                String adId = splited[3];
                String time = splited[0];
//                TODO 后续需要重构代码实现时间戳和分钟的转换提取，此处需要提取出AD的点击分钟单位
                return new Tuple2<>(time + "_" + adId, 1L);
            }
        }).reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 - v2;
            }
        }, Durations.minutes(30), Durations.minutes(1)).foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> partition) throws Exception {
                        List<AdTrendStat> adTrend = new ArrayList<>();
                        while (partition.hasNext()) {
                            Tuple2<String, Long> record = partition.next();
                            String[] splited = record._1.split("_");
                            String time = splited[0];
                            String adId = splited[1];
                            Long clicledCount = record._2;
                            /**
                             * 在插入数据库的时候具体哪些字段，time,adId,clickedCount
                             * 但是在统计绘图的时候需要年月日时分维度，所以我们应该要有这些维度
                             */
                            AdTrendStat adTrendStat = new AdTrendStat();
                            adTrendStat.setAdId(adId);
                            adTrendStat.setClickedCount(clicledCount);
                            adTrendStat.setDate(time);
                            adTrendStat.setHour(time);
                            adTrendStat.setMinute(time);
                            adTrend.add(adTrendStat);
                        }
                        final List<AdTrendStat> inserting = new ArrayList<>();
                        final List<AdTrendStat> updating = new ArrayList<>();
                        JDBCWrapper jdbcWrapper = JDBCWrapper.getInstance();
                        //AD点击趋势表字段：date,hour,minute,adId,clickedCount
                        for (final AdTrendStat clicked : adTrend) {
                            final AdTrendCountHistory adTrendCountHistory = new AdTrendCountHistory();
                            jdbcWrapper.doQuery(
                                    "select count(1) from t_adclicked_trend where date=? and hour=? and minute=? and ad_id=?",
                                    new Object[]{clicked.getDate(),
                                            clicked.getHour(),
                                            clicked.getMinute(),
                                            clicked.getAdId()
                                    }, new ExecuteCallBack() {
                                        @Override
                                        public void resultCallBack(ResultSet resultSet) throws SQLException {
                                            if (resultSet.getRow() != 0) {
                                                long count = resultSet.getLong(1);
                                                adTrendCountHistory.setClickedCountHistory(count);
                                                updating.add(clicked);
                                            } else {
                                                inserting.add(clicked);
                                            }
                                        }
                                    }
                            );

                        }
                        /**
                         * 数据库里面添加数据
                         */
                        ArrayList<Object[]> insertParamList = new ArrayList<>();
                        for (AdTrendStat insertRecord : inserting) {
                            insertParamList.add(new Object[]{
                                    insertRecord.getDate(),
                                    insertRecord.getHour(),
                                    insertRecord.getMinute(),
                                    insertRecord.getAdId(),
                                    insertRecord.getClickedCount()
                            });
                        }
                        jdbcWrapper.doBatch("insert into t_adclicked_trend values(?,?,?,?,?)",insertParamList);
                        /**
                         * 数据库更新数据
                         */
                        ArrayList<Object[]> updateParamList = new ArrayList<>();
                        for (AdTrendStat updateRecord:updating                             ) {
                            updateParamList.add(new Object[]{
                                    updateRecord.getClickedCount(),
                                    updateRecord.getDate(),
                                    updateRecord.getHour(),
                                    updateRecord.getMinute(),
                                    updateRecord.getAdId()
                            });
                        }
                        jdbcWrapper.doBatch("update t_adclicked_trend set clicked_count=? where date=? and hour=? and minute=? and ad_id=?",updateParamList);
                    }
                });
            }
        });

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        jsc.close();
    }


}
