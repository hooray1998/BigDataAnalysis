package eCommerceAnalyse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DaySalesNum {
	
	private static class DaySalesNumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split("\\s+");
			if (Integer.parseInt(words[1])==1) {
				outputKey.set(words[4].substring(5));
				context.write(outputKey, one);
			}
			
		}
		
		
	}
	
	private static class DaySalesNumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private IntWritable result = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job = Job.getInstance(conf, "DaySalesNumJob");
		job.setJarByClass(DaySalesNum.class);
		
		//设置专属的map和reduce
		job.setMapperClass(DaySalesNumMapper.class);
		job.setReducerClass(DaySalesNumReducer.class);
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//读取数据的格式（标准）<0,11 22 aa> <1,11 33 ss>
		job.setInputFormatClass(TextInputFormat.class);//还有keyvalue形式
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
		Path outputPath = new Path("/Test/DaySalesNum");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath, true);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//三目运算符 如果是true的话，就是取前面的0，否则取1
		System.exit(job.waitForCompletion(true)?0:1);
	}
		
}

