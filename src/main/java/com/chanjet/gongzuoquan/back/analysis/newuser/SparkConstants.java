package com.chanjet.gongzuoquan.back.analysis.newuser;

import com.chanjet.gongzuoquan.configcenter.ZkHelp;
import com.chanjet.gongzuoquan.utils.StringUtils;
import com.github.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhailzh
 * 
 * @Date 2016年1月4日——下午3:37:00
 * 
 */
public class SparkConstants {

//  static {
//    System.setProperty("config.type", "inte");
//  }

  private static String sparkzkpath = null;
  private static ZkHelp zkHelp = ZkHelp.getInstance();
  public static String zklist = null;
  public static final Logger logger = LoggerFactory.getLogger(SparkConstants.class);

  private static class InstanceHolder {
    private static final SparkConstants instance = new SparkConstants();
  }

  public static SparkConstants getInstance() {
    return InstanceHolder.instance;
  }

  private SparkConstants() {
    IZkDataListener listener = new IZkDataListener() {
      public void handleDataDeleted(String dataPath) throws Exception {
        logger.info("!!! kafkalog node data has been deleted !!!" + dataPath);
      }

      public void handleDataChange(String dataPath, byte[] data) throws Exception {
        logger.info("!!! kafkalog node data has been changed !!!" + dataPath);
        String kafkalogdata = StringUtils.toStr(data);
        sparkzkpath = kafkalogdata;
        logger.info("!!! kafkalog node data has been changed ok !!!" + kafkalogdata);
      }
    };
    // 节点添加监控
    String kafkalogZkPath = "/gongzuoquan/spark";
    zkHelp.subscribeDataChanges(kafkalogZkPath, listener);
    sparkzkpath = StringUtils.toStr(zkHelp.getValue(kafkalogZkPath));
    zklist = zkHelp.zooKeeperCluster;
  }

  /**
   * 获取kafkalog节点信息 0 brokerListValue 1 topic 2 kafka zk path
   */
  public String getSparkzkpath() {
    return sparkzkpath;
  }

  public static void main(String args[]) {
    SparkConstants rs = SparkConstants.getInstance();
    String value = rs.getSparkzkpath();
    System.out.println(value);
  }
}
