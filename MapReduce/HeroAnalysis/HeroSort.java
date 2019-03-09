package HeroAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 *
 * 7.27
 * 输入文件：HeroInfo
 * 输入格式：12	 109 50	45.87%
 *     	   hero  场次    胜场    胜率
 * 
 * 实现排序
 * 排序原则：胜率>胜场>玩家编号(升序)
 * 输出全服前十
 * 
 */
public class HeroSort {
	public static class HeroSortMapper extends Mapper<Text, Text, Text, NullWritable>{
		
		private Text outputKey = new Text();
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			outputKey.set(key.toString()+"\t"+value.toString());
			context.write(outputKey, NullWritable.get());
		}
	}
	public static class HeroSortReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		int num = 0;
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			if(num++<10){
				context.write(key, NullWritable.get());
			}
		}
	}
	
	public static class RankSort extends WritableComparator{
		public RankSort() {
			super(Text.class,true);
		}
		
		// 12  50 45.87 109
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String[] awords = ((Text)a).toString().split("\\s+");
			String[] bwords = ((Text)b).toString().split("\\s+");
			
			Double ar = Double.parseDouble(awords[3]);//rate
			Double br = Double.parseDouble(bwords[3]);
			
			Integer ac = Integer.parseInt(awords[1]);//win
			Integer bc = Integer.parseInt(bwords[1]);
			
			String aname = awords[0];
			String bname = bwords[0];
//			System.out.println(ar);
//			System.out.println(br);
			
			if(br.equals(ar)){
				if(bc.equals(ac)){
					return aname.compareTo(bname);
				}
				else{
					return bc.compareTo(ac);
				}
		      } else {
		        return br.compareTo(ar);
			}
		}
	}
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");//设置端口

			Job job;
			job=Job.getInstance(conf, "HeroSort");
			job.setJarByClass(HeroSort.class);

			job.setMapperClass(HeroSortMapper.class);
			job.setReducerClass(HeroSortReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			
			job.setSortComparatorClass(RankSort.class);
			job.setGroupingComparatorClass(RankSort.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);	

			//多文件输入
			Path inputPath1 = new Path("/HERO/AllHeroInfo");
			FileInputFormat.addInputPath(job,inputPath1);			
			
			Path outputPath = new Path("/HERO/AllHeroTop10");
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.exit(job.waitForCompletion(true)?0:1);
		} catch (Exception e) {
			e.printStackTrace();
		}				
	}
}