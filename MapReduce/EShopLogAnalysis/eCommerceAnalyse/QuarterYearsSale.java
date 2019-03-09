package eCommerceAnalyse;
/**
 * 
 * 统计各个季度(共四个季度)的销售量，销售额。
 * 第一季度：1--3月
 * 第二季度：4--6月
 * 第三季度：7--9月
 * 第四季度：10--12月
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


public class QuarterYearsSale {
	
	public static class QuarterYearsSalesMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outputKey = new Text();
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] words = value.toString().split("\\s+");
			if (Integer.parseInt(words[1]) == 1) {
				if (words[4].substring(5, 7).endsWith("01") || words[4].substring(5, 7).endsWith("02") || words[4].substring(5, 7).endsWith("03") ) {
					outputKey.set("第一季度");
					context.write(outputKey, value);
				}else if (words[4].substring(5, 7).endsWith("04") || words[4].substring(5, 7).endsWith("05") || words[4].substring(5, 7).endsWith("06")) {
					outputKey.set("第二季度");
					context.write(outputKey, value);
				}else if (words[4].substring(5, 7).endsWith("07") || words[4].substring(5, 7).endsWith("08") || words[4].substring(5, 7).endsWith("09")) {
					outputKey.set("第三季度");
					context.write(outputKey, value);
				}else {
					outputKey.set("第四季度");
					context.write(outputKey, value);
				}
			}
		}
		
	}
	
	public static class QuarterYearsSalesReducer extends Reducer<Text, Text, Text, NullWritable> {
		
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
			
			outputKey.set(key + "\t\t销售额："+str+"\t\t销量："+sale_num);
			context.write(outputKey, NullWritable.get());
			
		}
		
	}
	
	public static void main(String[] args) {
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口

			Job job;
			job=Job.getInstance(conf, "QuarterYearsSales");
			job.setJarByClass(QuarterYearsSale.class);

			job.setMapperClass(QuarterYearsSalesMapper.class);
			job.setReducerClass(QuarterYearsSalesReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);	
			

			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/QuarterYearsSales");

			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
