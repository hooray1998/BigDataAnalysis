package HeroAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * 计算玩家的KD
 */
public class PlayerKD {
	public static class PlayerKDMapper extends Mapper<Text, Text, Text, DoubleWritable>{
		
		private Text outputKey = new Text();
		private DoubleWritable outputValue = new DoubleWritable();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			String keyword = value.toString().split("\\s+")[1];
			Integer valueword = Integer.parseInt(value.toString().split("\\s+")[4]);
			Integer valueword2 = Integer.parseInt(value.toString().split("\\s+")[5]);
			outputKey.set(keyword);
			if(valueword2.equals(0))//除数为0
				valueword2 = 1;
			outputValue.set(1.0*valueword/valueword2);
			context.write(outputKey, outputValue);
		}
	}
	public static class PlayerKDReducer extends Reducer<Text, DoubleWritable, Text, Text>{
		
		private Text outputValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {

			double sum = 0;
			int count = 0;
			for (DoubleWritable value: values) {
				count++;
				sum+=value.get();
			}

			String out = String.format("%.2f", sum/count);
			outputValue.set(out);
			context.write(key, outputValue);
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		Job job;
		job=Job.getInstance(conf, "PlayerKD");
		job.setJarByClass(PlayerKD.class);
		
		//设置专属的map和reduce
		job.setMapperClass(PlayerKDMapper.class);
		job.setReducerClass(PlayerKDReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//设置文件的输入和输出位置
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		Path outputPath = new Path("/HERO/PlayerKD");
		//如果输出路径已存在，先删除
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}