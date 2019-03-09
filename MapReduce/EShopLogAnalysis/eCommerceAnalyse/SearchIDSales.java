package eCommerceAnalyse;

/**
 * 传参数查询某个ID的每年的购买额和购买力
 * 
 * SearchIDInfo.java查询某一个ID的相关信息
 * 选择的ID是
 * 0040ffc5-8954-4b28-96a1-21f5e8d274e8
 * 在这里直接用了查询到的这个ID的相关信息
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
import org.apache.log4j.BasicConfigurator;



/**
 * @author Administrator
 *   0	1	2015-05-15	2015-06-22	7	1201	80385.78	41580548441010511A5161
 */
public class SearchIDSales {
	public static class everyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		Text outputkey = new Text();
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String date = value.toString().split("\\s+")[3];
			String year = date.split("-")[0];
			outputkey.set(year);
			context.write(outputkey, value);
		}
	}
	//0	1	2015-05-15	2015-06-22	7	1201	80385.78	41580548441010511A5161
	public static class everyReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		double Money =0.00 ;
		Text outputkey=new Text();
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				if (value.toString().split("\\s+").length>=5) {
					if (value.toString().split("\\s+")[0].equals("1")) {
						Money+=Double.parseDouble(value.toString().split("\\s+")[6]);
				//		System.out.println("money"+Money);
					}
				}
		
			}
			outputkey.set(key+"     "+Money);
			context.write(outputkey,NullWritable.get());	
			Money=0.00;
		}
	}
	
	public static class Compare extends WritableComparator{
		public Compare(){
			super(Text.class,true);
		}
		

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Text akey = (Text)a;
			Text bkey = (Text)b;
			String acompare=akey.toString();
			String bcompare=bkey.toString();
			
			Integer aInteger= Integer.parseInt(acompare);
			Integer bInteger= Integer.parseInt(bcompare);
			
			if (aInteger.equals(bInteger)) {
				return 1;
			} else {
				return aInteger.compareTo(bInteger);
			}
		}
		
	}
	
	public static void main(String[] args) {
		try {
			
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job;
			job=Job.getInstance(conf, "SearchIDSales");
			job.setJarByClass(SearchIDSales.class);
			BasicConfigurator.configure();
			
			//设置专属的map和reduce
			job.setMapperClass(everyMapper.class);
			job.setReducerClass(everyReducer.class);
			//设置map的输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//设置reduce的输出类型			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(TextInputFormat.class);
			
			//设置文件的输入和输出位置
			FileInputFormat.addInputPath(job, new Path("/Test/SearchIDInfo/part-r-00000"));
			Path outputPath = new Path("/Test/SearchIDSales");
			//如果输出路径已存在，先删除
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setSortComparatorClass(Compare.class);
			job.setGroupingComparatorClass(Compare.class);
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}
