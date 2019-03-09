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

public class SearchPlayerKDA_Position {
	private static int id;
	 
	
	public static class SearchPlayerKDA_PositionMapper extends Mapper<Text, Text, Text, NullWritable> {
		private Text outputkey = new Text();
		
		private String Name[] = new String[5];
		private Integer AllCount[] = new Integer[5];
		private Double winCount[] = new Double[5];
		private int locateNum = 0;
		
		
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
				
				String locatename = value.toString().split("\\s+")[2];
				String kill= value.toString().split("\\s+")[4];
				String  die= value.toString().split("\\s+")[5];
				boolean find = false;
				for(int i=0;i<locateNum;i++){
					if(Name[i].equals(locatename)){
						find = true;
						AllCount[i]++;
						if(die.equals("0"))
						winCount[i]+=Double.parseDouble(kill);
						else
						winCount[i]+=Double.parseDouble(kill)/Double.parseDouble(die);
						break;
					}
				}
				if(!find){
					Name[locateNum] = locatename;
						AllCount[locateNum] = 0;
						winCount[locateNum] = 0.0;
						AllCount[locateNum]++;
						winCount[locateNum]+=Double.parseDouble(kill)/Double.parseDouble(die);
					locateNum++;
				}
		}
		
		@Override
		protected void cleanup(Mapper<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for(int i=0;i<locateNum;i++){
				String key = String.format("%-25s%.2f", Name[i], winCount[i]/AllCount[i] );
				System.out.println(key);
				outputkey.set(key);
				context.write(outputkey, NullWritable.get());
			}
		}
	}
	public static class SearchPlayerKDA_PositionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		
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
			
			Double ar = Double.parseDouble(awords[1]);//rate
			Double br = Double.parseDouble(bwords[1]);
			String aname = awords[0];
			String bname = bwords[0];
			
			if(br.equals(ar)){
				if(bname.equals(aname)){
					return 0;
				}
				else{
					return bname.compareTo(aname);
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
		job = Job.getInstance(conf, "SearchPlayerKDA_Position");
		job.setJarByClass(SearchPlayerKDA_Position.class);
		
		job.setMapperClass(SearchPlayerKDA_PositionMapper.class);
		job.setReducerClass(SearchPlayerKDA_PositionReducer.class);
		
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
		Path outputPath = new Path("/HERO/SearchPlayerKDA_Position/Player_"+id);
		FileSystem.get(conf).delete(outputPath,true);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}