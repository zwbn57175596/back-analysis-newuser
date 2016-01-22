package com.chanjet.gongzuoquan.back.analysis.newuser;

import com.chanjet.gongzuoquan.dao.pg.base.BaseDbcpDao;
import com.chanjet.gongzuoquan.dao.pg.model.CallBack;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    String date = "201601*/";
    // get hdfs path
    String path = hadoopurl + paths + date;

    // step 3
    // get file RDD
    long time = System.currentTimeMillis();
    JavaRDD<String> lines = ctx.textFile(path + "*");
    long time1 = System.currentTimeMillis();
    logger.info("zhaoweih load file cost: {}", (time1 - time));

    final Pattern p = Pattern.compile("(\"requestUrl\"\\:)\"[/[\\w]+]+\"");

    // count how many lines get
//    logger.info("zhaoweih title count: {}", lines.count());
    long time2 = System.currentTimeMillis();
    logger.info("zhaoweih count cost: {}", (time2 - time1));

    logA.info("before filter test");
    JavaRDD<String> afterFilter =
        lines.filter(new Function<String, Boolean>() {
          @Override
          public Boolean call(String v1) {
            logA.info("str for match: {}", v1);
            try {
              Matcher m = p.matcher(v1);
              if (m.find()) {
                String s = m.group().split("[:]")[1].replaceAll("[\"]", "");
                logA.info("uri str:{}", s);
                return BUSI_URI_SET.contains(s);
              }
              return false;
            } catch (Exception e) {
              logger.error("match", e);
            }
            return false;
          }
        });


    List<String> result = afterFilter.collect();
    for (String s : result) {
      logA.info("result str: {}", s);
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
