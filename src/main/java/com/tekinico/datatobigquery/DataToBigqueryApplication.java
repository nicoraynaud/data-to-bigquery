package com.tekinico.datatobigquery;

import com.tekinico.datatobigquery.service.DataExtractorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DataToBigqueryApplication {

	@Autowired
	private DataExtractorService dataExtractorService;

	public static void main(String[] args) {
		SpringApplication.run(DataToBigqueryApplication.class, args);
	}

}
