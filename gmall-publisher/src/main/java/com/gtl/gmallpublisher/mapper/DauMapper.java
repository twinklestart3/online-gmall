package com.gtl.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * 从数据库HBase查询数据的接口
 */
public interface DauMapper {
    // 查询日活总数
    long getDauTotal(String date);

    // cha查询小时明细
    List<Map> getDauHour(String date);
}
