package com.chanjet.gongzuoquan.back.analysis.newuser;

import com.chanjet.gongzuoquan.dao.pg.base.BaseDbcpDao;
import com.chanjet.gongzuoquan.dao.pg.model.CallBack;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.chanjet.gongzuoquan.back.analysis.newuser.BusiDataUriCollection.BUSI_URI_SET;

/**
 * 任务执行起始入口
 * Created by zhaowei on 1/14/16.
 */
public class TaskMain {

  private static final Logger logger = LoggerFactory.getLogger(TaskMain.class);

  private static final Logger logA = LoggerFactory.getLogger("stdout");

  private static final String paths = "/user/logs/aop/";
  private static final String hadoopurl = SparkConstants.getInstance().getSparkzkpath();

  private static final BaseDbcpDao baseDbcpDao = new BaseDbcpDao("/gongzuoquan/back/pg");

  public static void main(String[] args) {

    /*
      step 1 获取当前时间点的所有新增用户

      step 2 初始化hdfs环境

      step 3 过滤要扫描的文件

      step 4 RDD映射

      step 5 用<step 1>的数据对<step 4>的结果进行处理并计数

      step 6 <step 5>的结果写回PG
     */

    // step 1
    //    FutureTask<Set<Long>> userIdSet = getTodayNewUserIds();

    // step 2
    // initial spark context
    SparkConf sparkConf = new SparkConf();
    sparkConf.setAppName("newUserAnalysis");
    @SuppressWarnings("resource")
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    // get current date
    String date = "20160115/";
    // get hdfs path
    String path = hadoopurl + paths + date;

    // step 3
    // get file RDD
    JavaRDD<String> lines = ctx.textFile(path + "*");

    final Pattern p = Pattern.compile("(\"requestUrl\"\\:)\"[/[\\w]+]+\"");

    // map to url rdd
    JavaRDD<String> mapRdd = lines.map(new Function<String, String>() {

      @Override
      public String call(String v1) throws Exception {
        Matcher m = p.matcher(v1);
        if (m.find()) {
          return m.group().split("[:]")[1].replaceAll("[\"]", "");
        }
        return "";
      }
    });

    // filter
    long time2 = System.currentTimeMillis();
    logA.info("before filter test");
    JavaRDD<String> afterFilter =
        mapRdd.filter(new Function<String, Boolean>() {
          @Override
          public Boolean call(String v1) {
            try {
              return null != v1 && (!"".equals(v1) && BUSI_URI_SET.contains(v1));
            } catch (Exception e) {
              logger.error("match", e);
            }
            return false;
          }
        });

    // try repartition
    afterFilter.repartition(10);

    // map to pair
    JavaPairRDD<String, Integer> pairRdd = afterFilter.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s, 1);
          }
        });


    // reduce
    JavaPairRDD<String, Integer> counts = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
      }
    });

    // output
    List<Tuple2<String, Integer>> result = counts.collect();
    for (Tuple2<String, Integer> s : result) {
      logA.info("result str: {}", s.toString());
    }


    long time3 = System.currentTimeMillis();
    logger.info("zhaoweih filter count cost: {}" + (time3 - time2));
  }

  /**
   * 查询当日新增用户
   * @return 当日新增用户userid set
   */
  @SuppressWarnings("unused")
  private static FutureTask<Set<Long>> getTodayNewUserIds() {
    FutureTask<Set<Long>> futureTask = new FutureTask<>(new Callable<Set<Long>>() {
      public Set<Long> call() throws Exception {

        // construct params
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        ArrayList params = new ArrayList();
        //noinspection unchecked
        params.add(cal.getTimeInMillis());

        baseDbcpDao
            .query(0, " SELECT t.user_id as userid FROM user_info t WHERE t.create_time >= ? ",
                params, new CallBack() {
                  public Object getResultObject(ResultSet rs) {
                    Set<Long> set = new HashSet<>();
                    try {
                      while (rs.next()) {
                        set.add(rs.getLong("userid"));
                      }
                    } catch (SQLException e) {
                      logger.error("", e);
                    }
                    return set;
                  }
                });
        return null;
      }
    });

    new Thread(futureTask).start(); // execute query
    return futureTask;
  }
}
