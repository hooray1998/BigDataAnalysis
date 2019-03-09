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

public class SearchPlayerPosition {
	private static int id;
	 
	
	public static class SearchPlayerPositionMapper extends Mapper<Text, Text, Text, NullWritable> {
		private Text outputkey = new Text();
		
		private String Name[] = new String[5];
		private Integer AllCount[] = new Integer[5];
		private Integer winCount[] = new Integer[5];
		private int locateNum = 0;
		
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
				
				String locatename = value.toString().split("\\s+")[2];
				String result = value.toString().split("\\s+")[0];
				boolean find = false;
				for(int i=0;i<locateNum;i++){
					if(Name[i].equals(locatename)){
						find = true;
						AllCount[i]++;
						if(result.equals("1"))
							winCount[i]++;
						break;
					}
				}
				if(!find){
					Name[locateNum] = locatename;
					AllCount[locateNum] = 1;
					if(result.equals("1"))
						winCount[locateNum] = 1;
					else
						winCount[locateNum] = 0;
					locateNum++;
				}
		}
		
		@Override
		protected void cleanup(Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for(int i=0;i<locateNum;i++){
				String key = String.format("%-25s%-8d%-4d%.2f %%", Name[i], winCount[i], AllCount[i], 100*1.0*winCount[i]/AllCount[i] );
				//System.out.println(key);
				outputkey.set(key);
				context.write(outputkey, NullWritable.get());
			}
		}
	}
	public static class SearchPlayerPositionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		private Text outputkey = new Text();
		private Text outputvalue = new Text();
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
				context.write(key, NullWritable.get());
		}
	}
	public static class Sort2 extends WritableComparator{
		public Sort2() {
			super(Text.class,true);
		}
		
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

	public static void main(String[] args) throws Exception {
		
		//args[]代表的是参数列表，默认以逗号或空格分割
		id = Integer.parseInt(args[0]);
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Job job;
		job = Job.getInstance(conf, "SearchPlayerPosition");
		job.setJarByClass(SearchPlayerPosition.class);
		
		job.setMapperClass(SearchPlayerPositionMapper.class);
		job.setReducerClass(SearchPlayerPositionReducer.class);
		
		job.setSortComparatorClass(Sort2.class);
		job.setGroupingComparatorClass(Sort2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		if(id>9){
			FileInputFormat.addInputPath(job, new Path("/HERO/PlayerPartition/part-r-000"+id));
		} else {
			FileInputFormat.addInputPath(job, new Path("/HERO/PlayerPartition/part-r-0000"+id));
		}
		Path outputPath = new Path("/HERO/SearchPlayerPosition/Player_"+id);
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}