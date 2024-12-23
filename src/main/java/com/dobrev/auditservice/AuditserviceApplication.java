package com.dobrev.auditservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@EnableAspectJAutoProxy
@SpringBootApplication
public class AuditserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(AuditserviceApplication.class, args);
	}

}
