package com.tekinico.datatobigquery.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CsvResultSetExtractor implements ResultSetExtractor<Void> {

    private final File file;

    private List<Pair<String, String>> columns;

    /**
     * @param file the File to write the CSV to
     * @param columns the columns to export
     */
    public CsvResultSetExtractor(File file, List<Pair<String, String>> columns) {
        this.file = file;
        this.columns = columns;
    }

    @Override
    public Void extractData(final ResultSet rs) {
        try {
            FileWriter out = new FileWriter(file);
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);

            while (rs.next()) {
                List<Object> values = new ArrayList<>();
                for (Pair<String, String> col : columns) {
                    Object value;
                    switch(col.getSecond()) {
                        case "NUMERIC":
                            value = rs.getLong(col.getFirst());
                            break;
                        case "DATE":
                            Date date = rs.getDate(col.getFirst());
                            value = date != null ? date.toLocalDate() : null;
                            break;
                        case "STRING":
                        default:
                            value = rs.getString(col.getFirst());
                            break;
                    }
                    values.add(value);
                }

                printer.printRecord(values);
            }

            printer.close(true);

        } catch (IOException | SQLException ex) {
            // handle an error while reading dataset
        }
        return null;
    }
}
