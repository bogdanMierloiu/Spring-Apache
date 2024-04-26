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

import java.io.*;
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

    public List<LogsAverageResponse> analyzeLogsFromFile(List<MultipartFile> files) {
        JavaRDD<String> stringJavaRDD = readTextFile(files);
        JavaPairRDD<Double, String> averageMap = analyzeAppLogs(stringJavaRDD);
        return logsMapper.rddToDto(averageMap);
    }

    private JavaRDD<String> readTextFile(String path) {
        return sc.textFile(path);
    }

    private JavaRDD<String> readTextFile(List<MultipartFile> files) {
        saveFilesLocal(files);
        return sc.textFile("src/main/resources/api-logs/" + "aggregated_file.txt");
    }

    public static void saveFilesLocal(List<MultipartFile> files) {
        if (files != null && !files.isEmpty()) {
            File outputFile = new File("src/main/resources/api-logs/aggregated_file.txt");
            try (FileOutputStream os = new FileOutputStream(outputFile)) {
                for (MultipartFile file : files) {
                    appendFileContent(file, os);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void appendFileContent(MultipartFile file, OutputStream outputStream) throws IOException {
        try (InputStream is = file.getInputStream()) {
            byte[] buffer = new byte[4096];
            int readBytes;
            while ((readBytes = is.read(buffer)) > 0) {
                outputStream.write(buffer, 0, readBytes);
            }
            outputStream.write(System.lineSeparator().getBytes());
        }
    }

    private static JavaPairRDD<Double, String> analyzeAppLogs(JavaRDD<String> stringJavaRDD) {
//        String header = stringJavaRDD.first();
        JavaRDD<String> filteredData = stringJavaRDD.filter(row -> !row.trim().isEmpty());
        long count = filteredData.count();
        System.out.println("Total number of records: " + count);
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

        JavaPairRDD<String, Double> averageDurationPerEndpoint = endpointStatsReduced.mapValues(tuple -> tuple._2() / tuple._1());

        return averageDurationPerEndpoint.mapToPair(Tuple2::swap).sortByKey(false);
    }
}

