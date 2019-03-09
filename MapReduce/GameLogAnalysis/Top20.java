package game.log;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * @author zjzy
 *对用户按累计登录时间，次数及最早登陆时间排序取出前20
 */
public class Top20 {

	public static class Top20Mapper extends Mapper<LongWritable, Text,Text, NullWritable>{ 
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

				context.write(value, NullWritable.get());
		}
	}
	
	public static class Top20Reducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		
		private int num = 0
				;
	
		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
				if (num++ < 20) {
					context.write(key, NullWritable.get());
				}
		}
	}
	public static class Top20sort extends WritableComparator {
		
		public Top20sort() {
			super(Text.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			String[] awords = ((Text)a).toString().split("\\s+");
			String[] bwords = ((Text)b).toString().split("\\s+");
			
			Integer aTimes = Integer.parseInt(awords[1]);
			Integer bTimes = Integer.parseInt(bwords[1]);
			
			Integer aCount = Integer.parseInt(awords[2]);
			Integer bCount = Integer.parseInt(bwords[2]);
			
			return bTimes.compareTo(aTimes) == 0 ? bCount.compareTo(aCount) == 0 ? awords[3].compareTo(bwords[3]) : bCount.compareTo(aCount) : bTimes.compareTo(aTimes);
		}
	}
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "Top20");
		job.setJarByClass(Top20.class);
		
		job.setSortComparatorClass(Top20sort.class);
		job.setGroupingComparatorClass(Top20sort.class);
		
		job.setMapperClass(Top20Mapper.class);
		job.setReducerClass(Top20Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/nh-game-logs-3.5.1/part-r-00000"));
		
		Path outputPath = new Path("/nh-game-logs-3.5.2");
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
