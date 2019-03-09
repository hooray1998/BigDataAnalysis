package HeroAnalysis;

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
/*
 *
 * 7.26
 * 输入文件：game.log
 * 数据形式：Yi,0
 * 
 * 目的：分析英雄胜率
 * 
 */
public class AllHeroInfo {
	
	public static class AllHeroInfoMapper extends Mapper<Text, Text, Text, IntWritable>{
		
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split("\\s+");
			outputKey.set(key);
			outputValue.set(Integer.parseInt(words[0]));
			context.write(outputKey, outputValue);
		}
	}
	public static class AllHeroInfoReducer extends Reducer<Text, IntWritable, Text, Text>{
		
		private Text outputKey  = new Text();
		private Text outputValue  = new Text();
		private Integer one = new Integer(1); 
		
		private int HeroSum = 0;
		private int HeroWin = 0;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			for (IntWritable value : values) {
				HeroSum++;
				if(value.get()==1){
					HeroWin++;
				}
			}
			String WinningRate =String.format("%-8d\t%-4d\t%.2f",HeroSum,HeroWin, 1.0* HeroWin/HeroSum * 100)+" %";
			String newKey = String.format("%-20s", key);
			outputKey.set(newKey);
			outputValue.set(WinningRate);
			context.write(outputKey, outputValue);
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job=Job.getInstance(conf, "AllHeroInfo");
		job.setJarByClass(AllHeroInfo.class);
		
		//设置专属的map和reduce
		job.setMapperClass(AllHeroInfoMapper.class);
		job.setReducerClass(AllHeroInfoReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		Path outputPath = new Path("/HERO/AllHeroInfo");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
