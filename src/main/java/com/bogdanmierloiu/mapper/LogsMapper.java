package com.bogdanmierloiu.mapper;

import com.bogdanmierloiu.dto.LogsAverageResponse;
import org.apache.spark.api.java.JavaPairRDD;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

@Component
public class LogsMapper {

    private LogsMapper() {
    }

    public List<LogsAverageResponse> rddToDto(JavaPairRDD<Double, String> doubleStringJavaPairRDD) {
        List<Tuple2<Double, String>> collect = doubleStringJavaPairRDD.collect();
        List<LogsAverageResponse> logsAverageResponses = new ArrayList<>();
        for (Tuple2<Double, String> tuple : collect) {
            LogsAverageResponse response = new LogsAverageResponse(String.valueOf(tuple._1()), tuple._2());
            logsAverageResponses.add(response);
        }
        return logsAverageResponses;
    }
}
