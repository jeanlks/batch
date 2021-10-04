package com.batch.example.batchdemo;


import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.batch.core.launch.JobLauncher;

@Configuration
public class InputToDatabaseJob {
    @Autowired
    public JobBuilderFactory jobFactory;

    @Autowired
    public StepBuilderFactory stepFactory;

    @Autowired
    public PersonItemProcessor processor;

    @Autowired
	JobLauncher jobLauncher;

    @Autowired
    JobCompletionNotificationListener listener;


    @Autowired
    DataSource dataSource;


    @Bean
    public FlatFileItemReader<Person> reader(){
        return new FlatFileItemReaderBuilder<Person>()
            .name("personItemReader")
            .resource(new ClassPathResource("sample-data.csv"))
            .delimited()
            .names(new String[] {"firstName", "lastName"})
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
                { setTargetType(Person.class);}
            }).build();
    }


    @Bean
    public JdbcBatchItemWriter<Person> writer(){
        return new JdbcBatchItemWriterBuilder<Person>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO people(first_name, last_name) VALUES (:firstName, :lastName)")
            .dataSource(dataSource)
            .build();
    }

    @Bean
    public Job importUserJob() {
        return jobFactory.get("importToDatabase")
        .incrementer(new RunIdIncrementer())
        .listener(listener)
        .flow(stepImport())
        .end()
        .build();
    }

    @Bean
    public Step stepImport() {
        return stepFactory.get("step1").<Person,Person> chunk(10)
        .reader(reader())
        .processor(processor)
        .writer(writer())
        .build();
    }

    @Scheduled(cron = "*/5 * * * * *")
	public void perform() throws Exception {

		System.out.println("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();
        
        JobExecution execution =  jobLauncher.run(importUserJob(), param);

		System.out.println("Job finished with status :" + execution.getStatus());
	}
}
