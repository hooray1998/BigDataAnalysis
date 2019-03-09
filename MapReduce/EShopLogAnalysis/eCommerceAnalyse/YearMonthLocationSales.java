package eCommerceAnalyse;
/**
 * 通过传参数展示每年、每月、每地区的销售量销售额
 * 这里参数选择了
 * 2016年
 * 12月
 * 5省
 * 
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * @author Administrator
 *
 */
public class YearMonthLocationSales {
	
	private static String Year;
	private static String Month;
	private static String Province;
	
	
	
	public static class YearMonthLocationSalesMapper extends Mapper<Text, Text, Text, Text> {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class YearMonthLocationSalesReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		//某年销售量
		long buy_year;
		//某年销售额
		double money_year;
		//某月销售量
		long buy_month;
		//某月销售额
		double money_month;
		//某地区销售量
		long buy_place;
		//某地区销售额
		double money_place;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			for (Text value : values) {
				String message = value.toString();
				String time = message.split("\\s+")[3];
				String year = time.split("-")[0];
				String month = time.split("-")[1];
				String place = message.split("\\s+")[4];
				if (message.split("\\s+")[0].equals("1")) {
					if (year.equals(Year)) {
						buy_year++;
						money_year += Double.parseDouble(message.split("\\s+")[6]);
					}
					if (month.equals(Month)) {
						buy_month++;
						money_month += Double.parseDouble(message.toString().split("\\s+")[6]);
					}
					if (place.equals(Province)) {
						buy_place++;
						money_place += Double.parseDouble(message.toString().split("\\s+")[6]); 
					}
				}
			}
		}
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			Text outputkey = new Text();
			outputkey.set(Year+"年的销售量为: "+buy_year+"\t"+"销售额为: "+money_year+"\n"
							+Month+"月的销售量为: "+buy_month+"\t"+"销售额为: "+money_month+"\n"
							+Province+"省的销售量为: "+buy_place+"\t"+"销售额为: "+money_place);
			context.write(outputkey, NullWritable.get());
		}
	}
	public static void main(String[] args) {
		try {
			
			Year=args[0];
			Month=args[1];
			Province=args[2];
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job;
			job=Job.getInstance(conf, "YearMonthLocationSales");
			job.setJarByClass(YearMonthLocationSales.class);
			BasicConfigurator.configure();
			
			//设置专属的map和reduce
			job.setMapperClass(YearMonthLocationSalesMapper.class);
			job.setReducerClass(YearMonthLocationSalesReducer.class);
			//设置map的输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//设置reduce的输出类型			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			//设置文件的输入和输出位置
			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/YearMonthLocationSales");
			//如果输出路径已存在，先删除
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}

