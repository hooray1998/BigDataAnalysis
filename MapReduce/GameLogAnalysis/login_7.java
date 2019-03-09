package game.log;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author zjzy
 *通过传参的形式对authority_的权限处理，求出某天的登陆人数
 */
public class login_7 {
	
	private static int oneday;
	public static class login_7Mapper extends Mapper<Text, Text, Text, NullWritable> {
		
		private int day = 1<<(oneday-1);
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
				int flag = Integer.parseInt(value.toString());
				if((flag & day) == day){
					context.write(key, NullWritable.get());
				}
		}
	}
	public static class login_7Reducer extends Reducer<Text, NullWritable, IntWritable, NullWritable> {
		
		private IntWritable outputkey = new IntWritable();
		
		private int Sum = 0;
		//000208e3-0b15-48cc-bc87-aaf8d5bc48c4	121
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Sum ++;
		}
		@Override
		protected void cleanup(Reducer<Text, NullWritable, IntWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			outputkey.set(Sum);
			context.write(outputkey, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		
		//args[]代表的是参数列表，默认以逗号或空格分割
		oneday = Integer.parseInt(args[0]);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "login_7");
		job.setJarByClass(login_7.class);
		
		job.setMapperClass(login_7Mapper.class);
		job.setReducerClass(login_7Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/nh-game-logs-3.6/3.6.1"));
		
		Path outputPath = new Path("/nh-game-logs-3.7/3.7"+oneday);
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
