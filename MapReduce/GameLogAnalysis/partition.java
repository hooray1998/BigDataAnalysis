package game.log;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zjzy
 *以登陆时间的天分区
 */
public class partition {
	
	public static class partitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
	}
	
	public static class partitionReducer extends Reducer<Text, NullWritable,Text, NullWritable>{
		
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
			
			String beginTime = key.toString().split("\\s+")[3];
			String date = beginTime.split("T")[0];
			String[] dates = date.split("-");
			for (int i = 1; i < 8; i++) {
				if (dates[2].equals("0"+i)) {
					return(i-1)%numPartitions;
				}
			}
			return 0%numPartitions;
		}
	}
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "partition");
		job.setJarByClass(partition.class);
		
		job.setMapperClass(partitionMapper.class);
		job.setReducerClass(partitionReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/game-2017-01-01-2017-01-07.log"));
		
		Path outputPath = new Path("/nh-game-logs-3.2");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setNumReduceTasks(7);
		job.setPartitionerClass(PartitionPart.class);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
