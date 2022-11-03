package com.cognixia.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.cognixia.model.File;

public class FileRecordProcessor implements ItemProcessor<File, File> {

  private static final Logger log = LoggerFactory.getLogger(FileRecordProcessor.class);

  @Override
  public File process(final File record) throws Exception {

	  	final String fileid = record.getFileid();

	    final File transformedRecords = new File(fileid);

	    log.info("Converting (" + record + ") into (" + transformedRecords + ")");

	    return transformedRecords;
  }

}