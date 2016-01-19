package com.chanjet.gongzuoquan.back.analysis.newuser;

import java.util.HashSet;
import java.util.Set;

/**
 * 工作圈业务数据URI集合
 * Created by zhaowei on 1/14/16.
 */
public class BusiDataUriCollection {

  public static final Set<String> BUSI_URI_SET = new HashSet<String>(){{

    /*
      app module
     */
    add("/app/rest/referendum/saveJson");
    add("/app/web/referendum/saveJson");

    add("/app/rest/innerSign/add");
    add("/app/rest/notice/save");
    add("/app/rest/sign/add");

    add("/app/rest/workReport/add");
    add("/app/web/workReport/add");

    /*
      topic module
     */
    add("/quan/rest/topic/add");
    add("/quan/web/topic/add");

    /*
      comment module
     */
    add("/comment/rest/add");
    add("/comment/web/add");

  }};

}
