package com.yahoo.glimmer.indexing.generator;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Partition based only on the term. All occurrences of a term are processed by the same reducer instance. 
 */
public class FirstPartitioner extends HashPartitioner<TermOccurrencePair, Occurrence> {
    @Override
    public int getPartition(TermOccurrencePair key, Occurrence value, int numPartitions) {
        return Math.abs(key.getTerm().hashCode() * 127) % numPartitions;
    }
}