package eCommerceAnalyse;

/**
 * 
 * /**
 * @author Leowxm
 *按照订单的交易状态分区
 *  1为交易成功     0表示交易失败
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



public class StatuPartition {

	private static class StatuPartitioinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
	}
	
	
	private static class StatuPartitionReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			for (NullWritable value : values) {
				
				context.write(key, NullWritable.get());
			}
		}
	}
	
	private static class PartitionPart extends Partitioner<Text, NullWritable>{
		
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			
			String[] words = key.toString().split("\\s+");
			if (words[1].equals("0")) {
				return 0;
			}else {
				return 1;
			}
		}
	}
	
	public static void main(String[] args) {
		
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job;
			job = Job.getInstance(conf,"StatuPartition");
			job.setJarByClass(StatuPartition.class);

			job.setMapperClass(StatuPartitioinMapper.class);
			job.setReducerClass(StatuPartitionReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);	

			FileInputFormat.addInputPath(job, new Path("/Test/MoneyRangePartition/part-r-00006"));
			Path outputPath = new Path("/Test/MoneyRangeStatuPartition/Range6");
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setNumReduceTasks(2);
			job.setPartitionerClass(PartitionPart.class);

			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
