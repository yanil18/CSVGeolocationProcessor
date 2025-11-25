package com.csvgeolocationprocessor.CSVGeolocationProcessor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
public class MainController {

 // Inject IPinfo token from application.properties
    @Value("${ipinfo.token:}")
    private String ipinfoToken;

    @GetMapping("/")
    public String Homepage() {
        return "Homepage";
    }

    @PostMapping("/upload")
    public ResponseEntity<byte[]> uploadCsv(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return ResponseEntity.badRequest()
                .contentType(MediaType.TEXT_PLAIN)
                .body("Please select a file".getBytes());
        }
        
        if (!file.getOriginalFilename().endsWith(".csv")) {
            return ResponseEntity.badRequest()
                .contentType(MediaType.TEXT_PLAIN)
                .body("Please upload a CSV file".getBytes());
        }

        try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()));
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             CSVWriter writer = new CSVWriter(new OutputStreamWriter(baos))) {

            String[] header = reader.readNext();
            if (header == null) {
                return ResponseEntity.badRequest()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body("CSV file is empty".getBytes());
            }

            // Find lastip column
            int ipIndex = -1;
            for (int i = 0; i < header.length; i++) {
                if ("lastip".equalsIgnoreCase(header[i].trim())) {
                    ipIndex = i;
                    break;
                }
            }
            
            if (ipIndex == -1) {
                return ResponseEntity.badRequest()
                    .contentType(MediaType.TEXT_PLAIN)
                    .body("'lastip' column not found in CSV".getBytes());
            }

            // Add lat/lon columns
            List<String> headerList = new ArrayList<>();
            for (String col : header) {
                headerList.add(col);
            }
            headerList.add("latitude");
            headerList.add("longitude");
            writer.writeNext(headerList.toArray(new String[0]));

            // Process rows
            String[] row;
            RestTemplate restTemplate = new RestTemplate();
            int processedCount = 0;
            int successCount = 0;
            int failedCount = 0;
            
            while ((row = reader.readNext()) != null) {
                String ip = ipIndex < row.length ? row[ipIndex].trim() : "";
                String lat = "";
                String lon = "";

                if (!ip.isEmpty()) {
                    // Build URL with token if available
                    String url = buildIpinfoUrl(ip);
                    
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> data = restTemplate.getForObject(url, Map.class);
                        
                        if (data != null && data.containsKey("loc")) {
                            String loc = (String) data.get("loc");
                            String[] parts = loc.split(",");
                            if (parts.length == 2) {
                                lat = parts[0].trim();
                                lon = parts[1].trim();
                                successCount++;
                            }
                        }
                    } catch (Exception e) {
                        failedCount++;
                        if (e.getMessage().contains("429")) {
                            System.err.println("⚠️  Rate limit hit at row " + (processedCount + 1));
                        } else {
                            System.err.println("Error for IP " + ip + ": " + e.getMessage());
                        }
                    }
                    
                    // Increased delay to avoid rate limits
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                // Add row with lat/lon
                List<String> rowList = new ArrayList<>();
                for (String cell : row) {
                    rowList.add(cell);
                }
                rowList.add(lat);
                rowList.add(lon);
                writer.writeNext(rowList.toArray(new String[0]));

                processedCount++;
                if (processedCount % 10 == 0) {
                    System.out.println("Progress: " + processedCount + " rows processed");
                }
            }

            writer.flush();
            
            System.out.println("✓ Complete! Total: " + processedCount + 
                             " | Success: " + successCount + 
                             " | Failed: " + failedCount);

            byte[] bytes = baos.toByteArray();
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, 
                "attachment; filename=\"processed_" + file.getOriginalFilename() + "\"");
            headers.setContentType(MediaType.parseMediaType("text/csv"));

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(bytes);
                    
        } catch (IOException e) {
            System.err.println("IO Error: " + e.getMessage());
            return ResponseEntity.internalServerError()
                .contentType(MediaType.TEXT_PLAIN)
                .body(("Error: " + e.getMessage()).getBytes());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return ResponseEntity.internalServerError()
                .contentType(MediaType.TEXT_PLAIN)
                .body(("Error: " + e.getMessage()).getBytes());
        }
    }

    // Build URL with token if available
    private String buildIpinfoUrl(String ip) {
        if (ipinfoToken != null && !ipinfoToken.isEmpty()) {
            return "https://ipinfo.io/" + ip + "/json?token=" + ipinfoToken;
        }
        return "https://ipinfo.io/" + ip + "/json";
    }
}