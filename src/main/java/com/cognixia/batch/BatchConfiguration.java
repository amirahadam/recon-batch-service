package com.cognixia.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
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
import org.springframework.core.io.PathResource;

import com.cognixia.model.File;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public FlatFileItemReader<File> reader() {
		return new FlatFileItemReaderBuilder<File>()
			.name("recordItemReader")
			.resource(new PathResource("C:\\Users\\Amirah\\Documents\\javarepo\\file-transfer-service\\file-transfer-service\\recon.csv"))
//			.resource(new ClassPathResource("recon.csv"))
			.delimited()
			.names(new String[]{"fileid"})
			.fieldSetMapper(new BeanWrapperFieldSetMapper<File>() {{
				setTargetType(File.class);
			}})
			.build();
	}

	@Bean
	public FileRecordProcessor processor() {
		return new FileRecordProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<File> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<File>()
			.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
			.sql("DELETE FROM file_db.file WHERE file_id = (:fileid)")
			.dataSource(dataSource)
			.build();
	}

	@Bean
	public Job importUserJob(FileRecordProcessor listener, Step step1) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(step1)
			.end()
			.build();
	}

	@Bean
	public Step step1(JdbcBatchItemWriter<File> writer) {
		return stepBuilderFactory.get("step1")
			.<File, File> chunk(10)
			.reader(reader())
			.processor(processor())
			.writer(writer)
			.build();
	}

}