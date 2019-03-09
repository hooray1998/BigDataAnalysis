package eCommerceAnalyse;
/**
 * 对所有用户的购买情况进行统计和排名（累计消费金额>>累计消费次数）。取前二十。

 * @author leowxm
 * 
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


public class UserRanking {
	
	public static class UserRankingMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
		
	}
	
	public static class UserRankingReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		private int num = 0;
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			if(num++ < 20){
			context.write(key, NullWritable.get());
			}
		}
		
		
	}
	
	public static class Usercompareter extends WritableComparator {
		public Usercompareter(){
			super(Text.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			
			String[] awords = ((Text)a).toString().split("\\s+");
			String[] bwords = ((Text)b).toString().split("\\s+");
			
			Double amoney = Double.parseDouble(awords[1].substring(7));
			Double bmoney = Double.parseDouble(bwords[1].substring(7));
			
			Integer anum = Integer.parseInt(awords[2].substring(7));
			Integer bnum = Integer.parseInt(bwords[2].substring(7));
			
			return bmoney.compareTo(amoney) == 0?bnum.compareTo(anum) == 0?1:bnum.compareTo(anum):bmoney.compareTo(amoney);
			
		}
	}
	
	public static void main(String[] args) {

		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口
			
			Job job;
			job=Job.getInstance(conf, "UserRanking");
			job.setJarByClass(UserRanking.class);
			
			job.setMapperClass(UserRankingMapper.class);
			job.setReducerClass(UserRankingReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);	
			
			//设置排序规则
			job.setSortComparatorClass(Usercompareter.class);
			job.setGroupingComparatorClass(Usercompareter.class);	
			
			FileInputFormat.addInputPath(job, new Path("/Test/UserInfo/part-r-00000"));
			Path outputPath = new Path("/Test/UserRanking");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}		
		
	}
}
