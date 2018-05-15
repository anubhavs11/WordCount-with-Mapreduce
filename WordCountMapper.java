package org.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WordCountMapper extends MapReduceBase
implements Mapper<LongWritable,Text,Text,IntWritable>{
	
	public enum WordCounter{ //counter
		
		NO_CHARACTERS
		
	}
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter r)
			throws IOException {

		String val=value.toString();
		for (String word:val.split(" ")) {
			if(word.length()>0) {
				output.collect(new Text(word), new IntWritable(1));
			}
			if(word.length()==1) {
				r.incrCounter(WordCounter.NO_CHARACTERS, 1);
			}
		}
	}
}
