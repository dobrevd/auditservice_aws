package com.dobrev.auditservice.config;

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.AWSXRayRecorder;
import com.amazonaws.xray.AWSXRayRecorderBuilder;
import com.amazonaws.xray.jakarta.servlet.AWSXRayServletFilter;
import com.amazonaws.xray.strategy.sampling.CentralizedSamplingStrategy;
import jakarta.servlet.Filter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import java.io.FileNotFoundException;

@Configuration
@Slf4j
public class XrayConfig {
    public XrayConfig() {
        try {
            var ruleFile = ResourceUtils.getURL("classpath:xray/xray-sampling-rules.json");

            AWSXRayRecorder awsxRayRecorder = AWSXRayRecorderBuilder.standard()
                    .withDefaultPlugins()
                    .withSamplingStrategy(new CentralizedSamplingStrategy(ruleFile))
                    .build();

            AWSXRay.setGlobalRecorder(awsxRayRecorder);
        } catch (FileNotFoundException e) {
            log.error("Xray config file not found.");
        }
    }

    @Bean
    public Filter tracingFilter(){
        return new AWSXRayServletFilter("productsservice");
    }
}