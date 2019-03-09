package HeroAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.nh.mapreduce.newOldUser;

public class PlayerPartition {
	
	public static class PlayerPartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
	}
	
	public static class PlayerPartitionReducer extends Reducer<Text, NullWritable,Text, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			for (NullWritable value: values) {
				context.write(key, NullWritable.get());
			}
		}
	}
	
	public static class PartitionPart extends Partitioner<Text, NullWritable> {
		
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			
			String id = key.toString().split("\\s+")[2];
				return Integer.parseInt(id)%numPartitions;
		}
	}
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "PlayerPartition");
		job.setJarByClass(PlayerPartition.class);
		
		job.setMapperClass(PlayerPartitionMapper.class);
		job.setReducerClass(PlayerPartitionReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/HERO/HERO_NEW"));
		
		Path outputPath = new Path("/HERO/PlayerPartition");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(100);
		job.setPartitionerClass(PartitionPart.class);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
