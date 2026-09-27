package com.payment.ingestion;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingestion")
@RequiredArgsConstructor
public class IngestionController {

    private final FileIngestionService ingestionService;

    @Value("${app.processing.input-dir:./data/input}")
    private String inputDir;

    @PostMapping("/process")
    public ResponseEntity<Map<String, String>> processFile(@RequestParam(defaultValue = "sample-pacs009-batch.xml") String fileName) {
        Path filePath = Paths.get(inputDir, fileName);
        String jobId = ingestionService.processFile(filePath);

        return ResponseEntity.ok(Map.of(
                "jobId", jobId,
                "status", "COMPLETED",
                "message", "File processed and metadata published successfully."
        ));
    }
}