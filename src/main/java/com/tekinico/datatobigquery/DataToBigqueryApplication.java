package com.tekinico.datatobigquery;

import com.tekinico.datatobigquery.service.DataExtractorService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.util.Pair;

import java.io.File;
import java.util.*;

@SpringBootApplication
public class DataToBigqueryApplication implements InitializingBean {

	@Autowired
	private DataExtractorService dataExtractorService;

	public static void main(String[] args) {
		SpringApplication.run(DataToBigqueryApplication.class, args);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		List<Pair<String, String>> schema = Arrays.asList(
				Pair.of("subscriptionId", "NUMERIC"),
				Pair.of("dateBought", "DATE"),
				Pair.of("OS", "STRING")
		);

		String sqlQuery = "SELECT s.id as subscriptionId, s.dateBought, d.OS as deviceOS \n" +
						  "FROM subscription s INNER JOIN device d ON d.user_id = s.user_id";

		dataExtractorService.queryToBigQuery(sqlQuery, schema);
	}

}
