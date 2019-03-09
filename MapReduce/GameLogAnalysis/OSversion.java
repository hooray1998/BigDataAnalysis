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
 *所有设备的系统中的总次数及平均时长
 */
public class OSversion {
	
	public static class OSversionMapper extends Mapper<Text, Text, Text ,Text> {
		
		private Text outputValue =  new Text();
		private Text outputKey =  new Text();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			outputKey.set(value.toString().split("\\s+")[0]+"-"+value.toString().split("\\s+")[1]);
			outputValue.set(value.toString().split("\\s+")[4]);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class OSversionReducer extends Reducer<Text, Text, Text, Text> {
		private Text outputValue =  new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				int taotalplay = 0;
				double time = 0;
				for (Text value : values) {
					taotalplay++;
					time += Integer.parseInt(value.toString());
				}
			outputValue.set("总启动次数："+taotalplay+"\t"+"平均时长："+time/taotalplay);
			context.write(key, outputValue);
		}
		
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "OSversion");
		job.setJarByClass(OSversion.class);
		
		job.setMapperClass(OSversionMapper.class);
		job.setReducerClass(OSversionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path outputPath = new Path("/nh-game-logs-4.2");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
