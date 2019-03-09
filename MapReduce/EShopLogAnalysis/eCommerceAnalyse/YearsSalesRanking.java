package eCommerceAnalyse;

/**
 * 
 * 对这15年每年的销售情况做排名（消费金额>>消费次数）
 */
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

public class YearsSalesRanking {
	
	private static class YearsSalesRankingMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
		
	}
	
	private static class YearsSalesRankingReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			context.write(key, NullWritable.get());
		}
		
	}
	
	public static class Yearscompareter extends WritableComparator{
		
		public Yearscompareter(){
			super(Text.class,true);
		}
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			String[] awords = ((Text)a).toString().split("\\s+");
			String[] bwords = ((Text)b).toString().split("\\s+");
			
			Double amoney = Double.parseDouble(awords[1].substring(4));
			Double bmoney = Double.parseDouble(bwords[1].substring(4));
			
			Long anum = Long.parseLong(awords[2].substring(3));
			Long bnum = Long.parseLong(bwords[2].substring(3));
			
			return bmoney.compareTo(amoney) == 0?bnum.compareTo(anum) == 0?1:bnum.compareTo(anum):bmoney.compareTo(amoney);
			
		}
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口
			
			Job job;
			job=Job.getInstance(conf, "YearsSalesRanking");
			job.setJarByClass(YearsSalesRanking.class);
			
			job.setMapperClass(YearsSalesRankingMapper.class);
			job.setReducerClass(YearsSalesRankingReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);	
			
			//设置排序规则
			job.setSortComparatorClass(Yearscompareter.class);
			job.setGroupingComparatorClass(Yearscompareter.class);	
			
			FileInputFormat.addInputPath(job, new Path("/Test/YearsSales/part-r-00000"));
			Path outputPath = new Path("/Test/YearsSalesRanking");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
	}
	}