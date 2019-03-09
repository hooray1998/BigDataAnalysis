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


/**
 *利用已经按照省份地区进行分区过的数据，通过排名得到每个城市中销售额最多的20个市区。
 * @author leowxm
 *
 */

public class Top20CitiesSalesOfLocation {
	
	public static class Top20CitiesSalesOfLocationMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(value, NullWritable.get());
		}
		
	}
	
	public static class Top20CitiesSalesOfLocationReducer extends Reducer<Text,NullWritable, Text, NullWritable> {
		
		private int num = 0;
		
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			
			if(num++ < 20){
				context.write(key, NullWritable.get());
				}
		}
		
	}
	
	public static class Top20compareter extends WritableComparator{
		
		public Top20compareter(){
			super(Text.class,true);
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			
			String[] awords = ((Text)a).toString().split("\\s+");
			String[] bwords = ((Text)b).toString().split("\\s+");
			
			Double aMoney = Double.parseDouble(awords[2].substring(4));
			Double bMoney = Double.parseDouble(bwords[2].substring(4));
			
			Integer a_num = Integer.parseInt(awords[3].substring(3));
			Integer b_num = Integer.parseInt(bwords[3].substring(3));
			
			//降序降序升序
			return bMoney.compareTo(aMoney) == 0?b_num.compareTo(a_num) == 0? awords[3].compareTo(bwords[3]):b_num.compareTo(a_num):bMoney.compareTo(aMoney);
		}
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口
			
			Job job;
			job=Job.getInstance(conf, "Top20CitiesSalesOfLocation");
			job.setJarByClass(Top20CitiesSalesOfLocation.class);
			
			job.setMapperClass(Top20CitiesSalesOfLocationMapper.class);
			job.setReducerClass(Top20CitiesSalesOfLocationReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);	
			
			//设置排序规则
			job.setSortComparatorClass(Top20compareter.class);
			job.setGroupingComparatorClass(Top20compareter.class);	
			
			FileInputFormat.addInputPath(job, new Path("/Test/LocationCitiesSales/City9/part-r-00000"));
			Path outputPath = new Path("/Test/Top20CitiesSalesOfLocation/City9");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
}
