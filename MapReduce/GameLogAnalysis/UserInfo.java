package game.log;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 *outputkey.set("设备数：　"+totalDevice+"\t"+"登录次数：　"+totalplay+"\t"+"人均时长：　"+duration/totalDevice+"\t"+"次均时长：　"+duration/totalplay);
			context.write(outputkey, NullWritable.get());
 */
public class UserInfo {
	
	public static class UserInfoMapper extends Mapper<Text, Text, Text,Text> {
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			context.write(key, value);
		}
	}
	
	public static class UserInfoReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private Text outputkey = new Text();
		
		//设备数
		private int totalDevice = 0;
		
		//总次数
		private int totalplay = 0;
		
		//总时长
		private long duration = 0;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			//value不重复
			totalDevice++;
			for (Text value : values) {
				totalplay++;
				duration += Long.parseLong(value.toString().split("\\s+")[4]);
				
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			outputkey.set("设备数：　"+totalDevice+"\t"+"登录次数：　"+totalplay+"\t"+"人均时长：　"+duration/totalDevice+"\t"+"次均时长：　"+duration/totalplay);
			context.write(outputkey, NullWritable.get());
		}
		
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "UserInfo");
		job.setJarByClass(UserInfo.class);
		
		job.setMapperClass(UserInfoMapper.class);
		job.setReducerClass(UserInfoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path outputPath = new Path("/nh-game-logs-3.1");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
