package game.log;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zjzy
 *生成权限　得出用户在这几天的登陆信息
 *二进制串，从低到高代表从1号到7号，1表示登陆过
 */
public class authority_ {
	
	public static class authority_Mapper extends Mapper<Text, Text, Text ,Text> {
		
		private Text outputValue =  new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			outputValue.set(value.toString().split("\\s+")[2].substring(8, 10));
			context.write(key, outputValue);
		}
	}
	
	public static class authority_Reducer extends Reducer<Text, Text, Text, IntWritable> {
		
		private IntWritable outputvalue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

				int flag = 0;
				for (Text value : values) {
					int i = Integer.parseInt(value.toString());
					
					flag |= 1<<(i-1);
				}
				outputvalue.set(flag);
				context.write(key, outputvalue);
		}
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "authority_");
		job.setJarByClass(authority_.class);
		
		job.setMapperClass(authority_Mapper.class);
		job.setReducerClass(authority_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path outputPath = new Path("/nh-game-logs-3.6/3.6.1");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
