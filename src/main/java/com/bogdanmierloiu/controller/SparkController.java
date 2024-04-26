package com.bogdanmierloiu.controller;

import com.bogdanmierloiu.dto.LogsAverageResponse;
import com.bogdanmierloiu.service.SparkService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@RestController
@RequestMapping("/spark")
public class SparkController {


    private final SparkService sparkService;


    public SparkController(SparkService sparkService) {
        this.sparkService = sparkService;
    }


    @GetMapping("/analyze-internal-files")
    public List<LogsAverageResponse> analyzeInternalFiles() {
        return sparkService.analyzeInternalFiles();
    }

    @PostMapping
    public List<LogsAverageResponse> analyzeLogsFromFile(
            @RequestParam("files") List<MultipartFile> files) {
        return sparkService.analyzeLogsFromFile(files);
    }
}
