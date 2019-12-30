package com.gtl.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    // 查询总数
    long getDauTotal(String date);

    // 查询小时明细 相比数据层, 我们把数据结构做下调整, 更方便使用
    Map<String, Long> getDauHour(String date);

    // 销售额总数
    double getOrderAmountTotal(String date);

    // 销售额小时明细
    Map<String, Double> getOrderAmountHour(String date);
}
