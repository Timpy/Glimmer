package com.yahoo.glimmer.indexing;

import it.unimi.dsi.mg4j.document.Document;
import it.unimi.dsi.mg4j.document.DocumentFactory;
import it.unimi.dsi.mg4j.document.PropertyBasedDocumentFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



public class RDFInputFormat extends FileInputFormat<LongWritable, Document>  {
		  
		public static final String DOCUMENTFACTORY_CLASS = "documentfactory.class";

		
		@Override
		protected boolean isSplitable(JobContext context, Path file) {
			 CompressionCodec codec =  new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
		     return codec == null;
		}
		 
		@Override
		public RecordReader<LongWritable, Document> createRecordReader(InputSplit split,
				TaskAttemptContext context) {
			
			
			//Configure the document factory
			Class<?> documentFactoryClass = context.getConfiguration().getClass(DOCUMENTFACTORY_CLASS, PropertyBasedDocumentFactory.class);
			DocumentFactory factory = TripleIndexGenerator.initFactory(documentFactoryClass, context.getConfiguration(), null, true);
			
			return new RDFRecordReader(factory);
		}
	  }

