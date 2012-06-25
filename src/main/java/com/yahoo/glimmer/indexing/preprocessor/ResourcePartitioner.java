package com.yahoo.glimmer.indexing.preprocessor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ResourcePartitioner extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
	// TODO Auto-generated method stub
	return 0;
    }

}
