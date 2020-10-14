package com.tekinico.datatobigquery.service;

import com.tekinico.datatobigquery.util.CsvResultSetExtractor;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

@Service
public class DataExtractorService {

    private final JdbcTemplate jdbcTemplate;

    public DataExtractorService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public File queryToCSV(String sqlQuery, List<Pair<String, String>> schema) throws IOException {

        File csvFile = Files.createTempFile("extract_data", ".csv").toFile();

        jdbcTemplate.query(sqlQuery, new CsvResultSetExtractor(csvFile, schema));

        return csvFile;
    }
}
