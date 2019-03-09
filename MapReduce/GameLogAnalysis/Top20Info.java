package game.log;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.nh.mapreduce.UserInfo.UserInfoMapper;
import org.nh.mapreduce.UserInfo.UserInfoReducer;

/**
 * @author zjzy
 *记录玩家累计时长,次数以及最早登录时间
 */
public class Top20Info {
	
	public static class Top20InfoMapper extends Mapper<Text, Text, Text, Text> {
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class Top20InfoReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text outputvalue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			boolean flag = false;
			int times = 0;
			long time = 0;
			String first_time = " ";
			for (Text value : values) {
				
				times++;
				time += Integer.parseInt(value.toString().split("\\s+")[4]);
				if (!flag) {
					first_time = value.toString().split("\\s+")[2];
				}
				flag = true;
			}
			outputvalue.set(time+"\t"+times+"\t"+first_time);
			context.write(key, outputvalue);
		}
		/*@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				int playtime = 0;
				int playcount = 0;
				String firstTime = null;
				
				for (Text value : values) {
					String[] words = value.toString().split("\\s+");
					playcount++;
					playtime += Integer.parseInt(words[4]);
					if(playcount == 1){
						firstTime = words[2];
					}
				}
				outputvalue.set(playtime+"\t"+playcount+"\t"+firstTime);
				context.write(key, outputvalue);
		}*/
	}
	
		public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "Top20Info");
		job.setJarByClass(Top20Info.class);
		
		job.setMapperClass(Top20InfoMapper.class);
		job.setReducerClass(Top20InfoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path outputPath = new Path("/nh-game-logs-3.5.1");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}

