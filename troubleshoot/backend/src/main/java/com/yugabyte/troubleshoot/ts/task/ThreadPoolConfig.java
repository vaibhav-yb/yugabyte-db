package com.yugabyte.troubleshoot.ts.task;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class ThreadPoolConfig {

  @Bean("pgStatStatementsQueryExecutor")
  public ThreadPoolTaskExecutor taskExecutor(
      @Value("${task.pg_stat_statements_query.threads}") int threads) {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setCorePoolSize(threads);
    taskExecutor.setMaxPoolSize(threads);
    taskExecutor.setThreadNamePrefix("pss_query");
    taskExecutor.initialize();
    return taskExecutor;
  }
}
