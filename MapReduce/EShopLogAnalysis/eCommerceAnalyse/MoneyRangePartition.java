package eCommerceAnalyse;
/**
 * 根据消费金额进行分区
 * 
 * @author leowxm
 *
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

public class MoneyRangePartition {
	public static class MoneyRangePartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
	}
	
	public static class MoneyRangePartitionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			for (NullWritable value : values) {
				context.write(key, NullWritable.get());
			}
		}
	}
	
private static class  PartitionPart extends Partitioner<Text,NullWritable>{
		
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			//对一行数据进行分析
			String[] words = key.toString().split("\\s+");
			//找出是价格对应的字段并转化成float类型
			Float totalprice = Float.parseFloat(words[7]);
			if (totalprice < 5000) {
				return 0;
			}else if (totalprice < 10000) {
				return 1;
			}else if (totalprice < 20000) {
				return 2;
			}else if (totalprice < 30000) {
				return 3;
			}else if (totalprice < 40000) {
				return 4;
			}else if (totalprice < 50000) {
				return 5;	
			}else {
				return 6;
			}
		}
		
	}
	public static void main(String[] args) {
		
		try {			

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");

			Job job;
			job=Job.getInstance(conf, "userMoneyRangePartition");
			job.setJarByClass(MoneyRangePartition.class);

			job.setMapperClass(MoneyRangePartitionMapper.class);
			job.setReducerClass(MoneyRangePartitionReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);

			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/MoneyRangePartition");
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			job.setNumReduceTasks(7);
			job.setPartitionerClass(PartitionPart.class);
			
			System.exit(job.waitForCompletion(true)?0:1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
