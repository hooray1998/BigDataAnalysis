package eCommerceAnalyse;
/**
 * 查询某一个ID的相关信息
 * 选择的ID是
 * 0040ffc5-8954-4b28-96a1-21f5e8d274e8
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


/**
 * @author Administrator
 *
 */
public class SearchIDInfo {
	private static String ID ;
	public static class SearchIDInfoMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		Text outputkey = new Text();
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			outputkey.set(key.toString());
			context.write(outputkey, value);
		}
	}
	
	public static class SearchIDInfoReducer extends Reducer<Text, Text, Text, NullWritable> {

		private static String message;
		Text outputValue = new Text();
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (value.toString().split("\\s+")[0].equals(ID)) {
					message = value.toString();
					outputValue.set(message.split("\\s+")[1]+"\t"+message.split("\\s+")[2]+"\t"+message.split("\\s+")[3]+"\t"
							+message.split("\\s+")[4]+"\t"+message.split("\\s+")[5]+"\t"+message.split("\\s+")[6]+"\t"+message.split("\\s+")[7]+"\t"
							+message.split("\\s+")[8]);
					context.write(outputValue, NullWritable.get());
				}//outputValue.set("付款状态: "+message.split("\\s+")[1]+"\t"+"性别: "+message.split("\\s+")[2]+"\t"+"上线时间: "+message.split("\\s+")[3]+"\t"
			//	+"下单时间: "+message.split("\\s+")[4]+"\t"+"省份: "+message.split("\\s+")[5]+"\t"+"市区: "+message.split("\\s+")[6]+"\t"+"交易金额: "+message.split("\\s+")[7]+"\t"
			//	+"订单编号: "+message.split("\\s+")[8]);
			}

		}
		}
		
	public static void main(String[] args) {
		try {
			//arg[]代表的是参数列表，默认以空格或者逗号分隔
			ID = args[0];
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job;
			job=Job.getInstance(conf, "SearchIDInfo");
			job.setJarByClass(SearchIDInfo.class);
			BasicConfigurator.configure();
			
			//设置专属的map和reduce
			job.setMapperClass(SearchIDInfoMapper.class);
			job.setReducerClass(SearchIDInfoReducer.class);
			//设置map的输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//设置reduce的输出类型			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			
			//设置文件的输入和输出位置
			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/SearchIDInfo");
			//如果输出路径已存在，先删除
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
}
