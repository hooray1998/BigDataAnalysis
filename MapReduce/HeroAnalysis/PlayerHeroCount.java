package HeroAnalysis;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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


/*
 * 计算玩家的英雄数量 ，排名
 */
public class PlayerHeroCount {
	public static class PlayerHeroCountMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text outputKey = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String keyword = value.toString().split("\\s+")[1];
			outputKey.set(keyword);
			context.write(outputKey, key);
		}
	}
	public static class PlayerHeroCountReducer extends Reducer<Text, Text, Text, IntWritable>{
		
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Set<String> set = new HashSet<String>();
			for (Text value : values) {
				set.add(value.toString());
			}
			outputValue.set(set.size());
			context.write(key, outputValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job=Job.getInstance(conf, "PlayerHeroCount");
		job.setJarByClass(PlayerHeroCount.class);
		
		//设置专属的map和reduce
		job.setMapperClass(PlayerHeroCountMapper.class);
		job.setReducerClass(PlayerHeroCountReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		Path outputPath = new Path("/HERO/PlayerHeroCount");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
