package HeroAnalysis;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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


/*
 * 
 * 7.26
 * 输入文件：heros.log
 * 输入格式：
 * "Nunu,0"	#	57
 * 实现功能：总共英雄个数、玩家个数
 * 
 */
public class AllCount {
	public static class AllCountMapper extends Mapper<Text, Text, Text, Text>{
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] valueword = value.toString().split("\\s+");
			outputValue.set(valueword[1]+"="+valueword[7]);
			context.write(key, outputValue);
		}
	}
	public static class AllCountReducer extends Reducer<Text, Text, Text, NullWritable>{
		Set<String> set1 = new HashSet<String>();//hero
		Set<String> set2 = new HashSet<String>();//player
		Set<String> set3 = new HashSet<String>();//count
		
		private Text outputKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			set2.add(key.toString());
			for (Text value : values) {
				String[] words = value.toString().split("=");
				set1.add(words[0].toString());
				set3.add(words[1].toString());
			}
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			outputKey.set("游戏局数: " +set3.size() + "  玩家数量: " +set1.size() + "  英雄数量: " + set2.size());
			context.write(outputKey, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job=Job.getInstance(conf, "AllCount");
		job.setJarByClass(AllCount.class);
		
		//设置专属的map和reduce
		job.setMapperClass(AllCountMapper.class);
		job.setReducerClass(AllCountReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		Path outputPath = new Path("/HERO/AllCount");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
