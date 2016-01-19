package com.chanjet.gongzuoquan.back.analysis.newuser;

import com.chanjet.gongzuoquan.dao.pg.base.BaseDbcpDao;
import com.chanjet.gongzuoquan.dao.pg.model.CallBack;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * 任务执行起始入口
 * Created by zhaowei on 1/14/16.
 */
public class TaskMain {

  private static final Logger logger = LoggerFactory.getLogger(TaskMain.class);

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
    FutureTask<Set<Long>> userIdSet = getTodayNewUserIds();

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
    long time = System.currentTimeMillis();
    JavaRDD<String> lines = ctx.textFile(path + "*");
    long time1 = System.currentTimeMillis();
    System.out.println("zhaoweihLog load file cost：" + (time1 - time));

    // count how many lines get
    System.out.println("zhaoweihLog title count：" + lines.count());
    long time2 = System.currentTimeMillis();
    System.out.println("zhaoweihLog count cost ：" + (time2 - time1));

  }

  /**
   * 查询当日新增用户
   * @return 当日新增用户userid set
   */
  private static FutureTask<Set<Long>> getTodayNewUserIds() {
    FutureTask<Set<Long>> futureTask = new FutureTask<Set<Long>>(new Callable<Set<Long>>() {
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

        baseDbcpDao.query(0, " SELECT t.user_id as userid FROM user_info t WHERE t.create_time >= ? ",
            params, new CallBack() {
          public Object getResultObject(ResultSet rs) {
            Set<Long> set = new HashSet<Long>();
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
