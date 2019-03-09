package eCommerceAnalyse;

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


public class DaySalesNumTOP10 {
	
	private static class DaySalesNumTop10Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
		
	}
	
	private static class DaySalesNumTop10Reducer extends Reducer<Text, NullWritable,Text,NullWritable> {
		
		private int num = 0;
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			if(num++ < 10){
				context.write(key, NullWritable.get());
				}
		}
	}
	
	public static class Top10compareter extends WritableComparator{
		
		public Top10compareter(){
			super(Text.class,true);
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			
			String[] awords = ((Text)a).toString().split("\\s+");
			String[] bwords = ((Text)b).toString().split("\\s+");
			
//			Integer a_num = Integer.parseInt(awords[1]);
//			Integer b_num = Integer.parseInt(bwords[1]);
			
			Double a_num = Double.parseDouble(awords[1]);
			Double b_num = Double.parseDouble(bwords[1]);
			
			//降序降序升序
			return b_num.compareTo(a_num)==0?1:b_num.compareTo(a_num);
		}
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口
			
			Job job;
			job=Job.getInstance(conf, "DaySalesNumTOP10");
			job.setJarByClass(DaySalesNumTOP10.class);
			
			job.setMapperClass(DaySalesNumTop10Mapper.class);
			job.setReducerClass(DaySalesNumTop10Reducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);	
			
			//设置排序规则
			job.setSortComparatorClass(Top10compareter.class);
			job.setGroupingComparatorClass(Top10compareter.class);	
			
			FileInputFormat.addInputPath(job, new Path("/Test/DaySalesMoney/part-r-00000"));
			Path outputPath = new Path("/Test/DaySalesMoneyTop10");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
}
