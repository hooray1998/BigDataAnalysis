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
 * 计算玩家的平均评分
 */
public class PlayerGrade {
	public static class PlayerGradeMapper extends Mapper<Text, Text, Text, IntWritable>{
		
		private Text outputKey = new Text();
		private IntWritable outputValue = new IntWritable();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String keyword = value.toString().split("\\s+")[1];
			Integer valueword = Integer.parseInt(value.toString().split("\\s+")[6]);
			outputKey.set(keyword);
			outputValue.set(valueword);
			context.write(outputKey, outputValue);
		}
	}
	public static class PlayerGradeReducer extends Reducer<Text, IntWritable, Text, Text>{
		
		private Text outputValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			int count = 0;
			for (IntWritable value : values) {
				count++;
				sum+=value.get();
			}

			String out = String.format("%.2f", 1.0*sum/count);
			outputValue.set(out);
			context.write(key, outputValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job=Job.getInstance(conf, "PlayerGrade");
		job.setJarByClass(PlayerGrade.class);
		
		//设置专属的map和reduce
		job.setMapperClass(PlayerGradeMapper.class);
		job.setReducerClass(PlayerGradeReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		Path outputPath = new Path("/HERO/PlayerGrade");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}