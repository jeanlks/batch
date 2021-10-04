package com.batch.example.batchdemo;

import java.util.Date;

import javax.sql.DataSource;
import javax.xml.crypto.Data;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;

@Configuration
@EnableBatchProcessing
@EnableScheduling
public class OutputJobToFile {

    @Autowired
    JobBuilderFactory jobFactory;

    @Autowired
    StepBuilderFactory stepFactory;

    @Autowired
    PersonItemProcessor processor;

    @Autowired
	JobLauncher jobLauncher;

    @Autowired
    JobCompletionNotificationListener listener;

    @Autowired
    DataSource dataSource;

    @Bean
    public JdbcCursorItemReader<Person> databaseReader() {
        return new JdbcCursorItemReaderBuilder<Person>()
        .dataSource(dataSource)
        .name("databaseReader")
        .sql("SELECT first_name, last_name FROM people")
        .rowMapper(new PersonRowMapper())
        .build();
    }

        
    @Bean
    public FlatFileItemWriter fileWriter(){
        return new FlatFileItemWriterBuilder<Person>()
        .name("fileWriter")
        .lineAggregator(new DelimitedLineAggregator<Person>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {
                    {
                        setNames(new String[] { "firstName", "lastName" });
                    }
                });
            }
        })
        .resource(new FileSystemResource("src/main/resources/output.txt"))
        .build();
    }

    @Bean
    public Job toFileUserJob() {
        return jobFactory.get("importToDatabase")
        .incrementer(new RunIdIncrementer())
        .listener(listener)
        .flow(stepFile())
        .end()
        .build();
    }


    @Bean
    public Step stepFile() {
        return stepFactory.get("step2").<Person,Person> chunk(10)
        .reader(databaseReader())
        .processor(processor)
        .writer(fileWriter())
        .build();
    }

    @Scheduled(cron = "*/5 * * * * *")
	public void perform() throws Exception {

		System.out.println("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();
        
        JobExecution execution =  jobLauncher.run(toFileUserJob(), param);

		System.out.println("Job finished with status :" + execution.getStatus());
	}

}
