package com.bogdanmierloiu.service;

import com.bogdanmierloiu.dto.LogsAverageResponse;
import com.bogdanmierloiu.mapper.LogsMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

@Service
public class SparkService {

    private final JavaSparkContext sc;

    private final LogsMapper logsMapper;

    public SparkService(JavaSparkContext sc, LogsMapper logsMapper) {
        this.sc = sc;
        this.logsMapper = logsMapper;
    }

    public List<LogsAverageResponse> analyzeInternalFiles() {
        JavaRDD<String> stringJavaRDD = readTextFile("src/main/resources/api-logs/logs-24.04.2024.csv");
        JavaPairRDD<Double, String> averageMap = analyzeAppLogs(stringJavaRDD);
        return logsMapper.rddToDto(averageMap);

    }

    public Object[] analyzeLogsFromFile(MultipartFile file) {
        JavaRDD<String> stringJavaRDD = readTextFile("src/main/resources/api-logs/logs-24.04.2024.csv");
        JavaPairRDD<Double, String> doubleStringJavaPairRDD = analyzeAppLogs(stringJavaRDD);
        return doubleStringJavaPairRDD.collect().toArray();
    }


    private JavaRDD<String> readTextFile(String path) {
        return sc.textFile(path);
    }

//    private JavaRDD<String> readTextFile(MultipartFile file) {
//        return sc.textFile(file);
//    }

    private static JavaPairRDD<Double, String> analyzeAppLogs(JavaRDD<String> stringJavaRDD) {
        String header = stringJavaRDD.first();
        JavaRDD<String> filteredData = stringJavaRDD.filter(row -> !row.equals(header) && !row.trim().isEmpty());
        JavaRDD<Tuple3<String, String, String>> tuple3JavaRDD = filteredData.map(rawValue -> {
            String[] columns = rawValue.split(",");
            String latency = columns[0];
            String requestMethod = columns[4];
            String rawRequestUrl = columns[6];
            int startIndex = rawRequestUrl.indexOf("/", "https://".length());
            String requestUrl;
            if (startIndex != -1) {
                requestUrl = rawRequestUrl.substring(startIndex);
            } else {
                requestUrl = rawRequestUrl;
            }
            return new Tuple3<>(latency, requestMethod, requestUrl);
        });

        JavaRDD<Tuple3<String, String, String>> tuple3Filtered = tuple3JavaRDD
                .filter(tuple -> !tuple._1().isEmpty() && !tuple._2().isEmpty() && !tuple._3().isEmpty());
        JavaPairRDD<String, Tuple2<Integer, Double>> endpointStats = tuple3Filtered.mapToPair(tuple -> {
            String endpoint = tuple._3();
            double duration = Double.parseDouble(tuple._1().replace("s", ""));
            return new Tuple2<>(endpoint, new Tuple2<>(1, duration));
        });

        JavaPairRDD<String, Tuple2<Integer, Double>> endpointStatsReduced = endpointStats.reduceByKey((x, y) -> new Tuple2<>(x._1() + y._1(), x._2() + y._2()));

        // Calculăm durata medie a fiecărui apel pentru fiecare endpoint
        JavaPairRDD<String, Double> averageDurationPerEndpoint = endpointStatsReduced.mapValues(tuple -> tuple._2() / tuple._1());

        // Sortăm endpoint-urile în funcție de durata medie
        return averageDurationPerEndpoint.mapToPair(Tuple2::swap).sortByKey(false);
    }
}

