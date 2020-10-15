package com.tekinico.datatobigquery;

import com.tekinico.datatobigquery.service.DataExtractorService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class AppStartupRunner implements ApplicationRunner {

    private final DataExtractorService dataExtractorService;

    public AppStartupRunner(DataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Pair<String, String>> schema = Arrays.asList(
                Pair.of("subscriptionId", "NUMERIC"),
                Pair.of("dateBought", "DATE"),
                Pair.of("OS", "STRING")
        );

        String sqlQuery = "SELECT s.id as subscriptionId, s.date_bought as dateBought, d.OS as deviceOS \n" +
                "FROM subscription s INNER JOIN device d ON d.user_id = s.user_id";

        dataExtractorService.queryToBigQuery(sqlQuery, schema);
    }
}
