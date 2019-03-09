package HeroAnalysis2;

import java.io.IOException;

import javax.xml.transform.OutputKeys;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zjzy
 *英雄名对应id 0-29 作为分区
 */
public class partitionHero {
	
	public static class partitionHeroMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		private Text OutputKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String name = value.toString().split("\\s+")[0];
			int flag=0;;
			if (name.equals("Amumu")) {
				flag = 0;
			}
			if (name.equals("Darius")) {
				flag = 1;
			}
			if (name.equals("Fiora")) {
				flag = 2;
			}
			if (name.equals("Karthus")) {
				flag = 3;
			}
			if (name.equals("Varus")) {
				flag = 4;
			}
			if (name.equals("Alistar")) {
				flag = 5;
			}
			if (name.equals("Garen")) {
				flag = 6;
			}
			if (name.equals("Dr_Mundo")) {
				flag = 7;
			}
			if (name.equals("Fizz")) {
				flag = 8;
			}
			if (name.equals("Coin")) {
				flag = 9;
			}
			if (name.equals("Urgot")) {
				flag = 10;
			}
			if (name.equals("Malphite")) {
				flag = 11;
			}
			if (name.equals("Cho_Gath")) {
				flag = 12;
			}
			if (name.equals("Wukong")) {
				flag = 13;
			}
			if (name.equals("Zed")) {
				flag = 14;
			}
			if (name.equals("TwistedFate")) {
				flag = 15;
			}
			if (name.equals("MissFortune")) {
				flag = 16;
			}
			if (name.equals("Thresh")) {
				flag = 17;
			}
			if (name.equals("Poppy")) {
				flag = 18;
			}
			if (name.equals("Riven")) {
				flag = 19;
			}
			if (name.equals("Akali")) {
				flag = 20;
			}
			if (name.equals("Syndra")) {
				flag = 21;
			}
			if (name.equals("Draven")) {
				flag = 22;
			}
			if (name.equals("Braum")) {
				flag = 23;
			}
			if (name.equals("Udyr")) {
				flag = 24;
			}
			if (name.equals("Tryndamere")){
				flag = 25;
			}
			if (name.equals("Talon")) {
				flag = 26;
			}
			if (name.equals("Ahri")) {
				flag = 27;
			}
			if (name.equals("Vayne")) {
				flag = 28;
			}
			if (name.equals("Soraka")) {
				flag = 29;
			}
			OutputKey.set(flag+"-"+value);
			context.write(OutputKey, NullWritable.get());
		}
	}
	
	public static class partitionHeroReducer extends Reducer<Text, NullWritable,Text, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			Text outputKey = new Text();
			for (NullWritable value: values) {
				outputKey.set(key.toString().split("-")[1]);
				context.write(outputKey, NullWritable.get());
			}
		}
	}
	
	public static class PartitionPart extends Partitioner<Text, NullWritable> {
		
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			
			String id = key.toString().split("-")[0];
			for (int i = 0; i < 30; i++) {
				if (Integer.parseInt(id) == i) {
					return(i)%numPartitions;
				}
			}
			return 0%numPartitions;
		}
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "partitionHero");
		job.setJarByClass(PartitionPart.class);
		
		job.setMapperClass(partitionHeroMapper.class);
		job.setReducerClass(partitionHeroReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/HERO_NEW"));
		
		Path outputPath = new Path("/nh-hero-3.0");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(30);
		job.setPartitionerClass(PartitionPart.class);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
