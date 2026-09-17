package br.com.spring.batch.partitioner.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Profile("partitioner")
@EnableScheduling
public class SchedulingConfig {
}
