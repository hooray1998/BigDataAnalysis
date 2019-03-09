package eCommerceAnalyse;

/**
 * 将数据按照购买年份进行分区, 分成15份。
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EachYearCitiesSalesPartition {
	
	public static class EachYearCitiesSalesPartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
	}
	
	public static class EachYearCitiesSalesPartitionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			for (NullWritable value : values) {
				context.write(key, NullWritable.get());
			}
		}
	}
	
	public static class EachYearCitiesSalesPartitionPart extends Partitioner<Text, NullWritable> {

		// 9ccccfa6-86d1-46c3-b160-453f9a8ca4f2	iOS	10.3.2	2017-01-07T23:59:05	2017-01-07T23:59:21	16

		//001879cf-92b2-4cc0-875a-67b9f2e2de36	1	1	2017-11-15	2017-11-15	2	0803   	709.37   	44490737541010311A1001package org.nh.mapreduce;
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			
			String shopTime = key.toString().split("\\s+")[4];
			String[] dates = shopTime.split("-");
			for (int i = 2004; i < 2019; i++) {
				if (i == Integer.parseInt(dates[0])) {
					return (i-2004)%numPartitions;
				}
			}
			return 0%numPartitions;
		}
		
	}
	
	public static void main(String[] args) {
		
		try {			

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job;
			job=Job.getInstance(conf, "userEachYearCitiesSalesPartition");
			job.setJarByClass(EachYearCitiesSalesPartition.class);

			job.setMapperClass(EachYearCitiesSalesPartitionMapper.class);
			job.setReducerClass(EachYearCitiesSalesPartitionReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);

			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/YearsPartition");
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			job.setNumReduceTasks(15);
			job.setPartitionerClass(EachYearCitiesSalesPartitionPart.class);
			
			System.exit(job.waitForCompletion(true)?0:1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
