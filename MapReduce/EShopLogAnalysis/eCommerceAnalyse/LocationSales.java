package eCommerceAnalyse;
/**
 * 统计各地区(/按省份)的销售量、销售额
 * @author leowxm
 */

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LocationSales {
	
	public static class LocationSalesMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split("\\s+");
			
			if (Integer.parseInt(words[1]) == 1) {
				
				outputKey.set(words[5].substring(0, 1));
				context.write(outputKey, value);
			}
		}
		 
		
	}
	
	public static class LocationSalesReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private Text outputKey = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			double sale_money = 0.00;
			long sale_num = 0;
			
			for (Text value : values) {
				
				String[] words = value.toString().split("\\s+");
				
				sale_money += Double.parseDouble(words[6]);
				sale_num ++;
			}
			
			DecimalFormat df = new DecimalFormat("#####0.00"); 
			String str = df.format(sale_money);
			
			outputKey.set("地区："+key+"省"+ "\t\t销售额："+str+"\t\t销量："+sale_num);
			context.write(outputKey, NullWritable.get());
		}
		
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口

			Job job;
			job=Job.getInstance(conf, "LocationSales");
			job.setJarByClass(LocationSales.class);

			job.setMapperClass(LocationSalesMapper.class);
			job.setReducerClass(LocationSalesReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);	
			

			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/LocationSales");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}

}
