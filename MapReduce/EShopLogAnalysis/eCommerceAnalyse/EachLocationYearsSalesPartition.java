package eCommerceAnalyse;

/**
 * 将数据按照省份地区进行分区。
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EachLocationYearsSalesPartition {
	
	public static class EachLocationYearsSalesPartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		Text outputvalue = new Text();
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			//System.out.println("77777777777777777777777777777777777");
			outputvalue.set(value);
			context.write(outputvalue, NullWritable.get());
		}
	}
	
	public static class EachLocationYearsSalesPartitionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			for (NullWritable text : values) {
				context.write(key, NullWritable.get());
			}
		}
	}
	
	public static class getPartition2 extends Partitioner<Text, NullWritable> {

		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
		 */
		@Override
		public int getPartition(Text key, NullWritable value, int numPartitions) {
			if (key.toString().split("\\s+").length>=5) {
				String EachLocationYearsSalesPartition = key.toString().split("\\s+")[5];
				//System.out.println("*********************************"+EachLocationYearsSalesPartition);
				int pro = Integer.parseInt(EachLocationYearsSalesPartition);
			//	System.out.println("***********************************");
				for (int i = 0; i < 10; i++) {
					if (i == pro) {
						return (i)%numPartitions;
					}
				}
			}
			
			
			return 0;
		}
		
	}
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job;
			job=Job.getInstance(conf, "EachLocationYearsSalesPartition");
			job.setJarByClass(EachLocationYearsSalesPartition.class);
			//BasicConfigurator.configure();
			
			//设置专属的map和reduce
			job.setMapperClass(EachLocationYearsSalesPartitionMapper.class);
			job.setReducerClass(EachLocationYearsSalesPartitionReducer.class);
			
			//设置reduce的输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			
			//设置文件的输入和输出位置
			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/LocationPartition");
			//如果输出路径已存在，先删除
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setNumReduceTasks(10);
			job.setPartitionerClass(getPartition2.class);
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}
