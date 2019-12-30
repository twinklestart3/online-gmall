package com.gtl.gmallpublisher.service;

import com.gtl.gmallpublisher.mapper.DauMapper;
import com.gtl.gmallpublisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 必须添加 Service 注解
@Service
public class PublisherServiceImpl implements PublisherService {

    // 自动注入 DauMapper 对象
    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Override
    public long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHour(String date) {
        List<Map> dauHourList = dauMapper.getDauHour(date);
        Map<String, Long> dauHourMap = new HashMap<>();
        for (Map map : dauHourList) {
            String hour = (String) map.get("LOGHOUR");
            Long count = (Long) map.get("COUNT");
            dauHourMap.put(hour, count);
        }
        return dauHourMap;
    }

    @Override
    public double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderAmountHour(String date) {
        List<Map> orderAmountHour = orderMapper.getOrderAmountHour(date);
        Map<String, Double> orderHourAmountMap = new HashMap<>();
        for (Map map : orderAmountHour) {
            String hour = (String) map.get("CREATE_HOUR");
            Double count = ((BigDecimal) map.get("SUM")).doubleValue();
            orderHourAmountMap.put(hour, count);
        }
        return orderHourAmountMap;
    }
}
