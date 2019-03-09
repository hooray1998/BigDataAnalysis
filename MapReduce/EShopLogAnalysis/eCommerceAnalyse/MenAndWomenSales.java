package eCommerceAnalyse;

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
 * 统计男、女的销售额和销售量，并计算其比重。
 * @author leowxm
 *
 */
public class MenAndWomenSales {
	
	public static class MenAndWomenSalesMapper extends Mapper<Text, Text, Text, Text> {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	//0bf621f1-9aa4-42a8-9dc2-0c7adf63840b	41630758341020411A1001	D36.9`00	25	240d1d79-f44b-4431-9d74-d517c22b91d1	1	2014-03-15	2014-03-30	70559.51	0
	public static class MenAndWomenSalesReducer extends Reducer<Text, Text, Text, NullWritable> {

		//男性销售额总量
		double money_m =0;
		//女性销售额总量
		double money_w=0;
		//男性购买力
		long buy_m = 0;
		//女性购买力
		long buy_w=0;
		//gender
		int gender =0 ;
		//付款状态
		int buy =0;
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			for (Text value : values) {
				gender = Integer.parseInt(value.toString().split("\\s+")[1]);
				buy = Integer.parseInt(value.toString().split("\\s+")[0]);
				if (gender == 1&&buy == 1) {
					buy_m += 1;
					money_m += Double.parseDouble(value.toString().split("\\s+")[6]);
				}else if (gender == 2&&buy == 1) {
					buy_w += 1;
					money_w += Double.parseDouble(value.toString().split("\\s+")[6]);
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
			String money_m_Rate= String .format("%.2f",1.0*money_m/(money_m+money_w)*100)+"%";
			String money_w_Rate= String .format("%.2f",1.0*money_w/(money_m+money_w)*100)+"%";
			String buy_m_Rate= String .format("%.2f",1.0*buy_m/(buy_m+buy_w)*100)+"%";
			String buy_w_Rate= String .format("%.2f",1.0*buy_w/(buy_m+buy_w)*100)+"%";
			outputkey.set("男性销售额: "+money_m+"\t"+"男性购买力: "+buy_m+"\t"+"男性销售额占总销售额比重: "+money_m_Rate+"\t"+"男性购买力占总购买力比重: "+buy_m_Rate+"\n"+
							"女性销售额: "+money_w+"\t"+"女性购买力: "+buy_w+"\t"+"女性销售额占总销售额比重: "+money_w_Rate+"\t"+"女性购买力占总购买力比重: "+buy_w_Rate);
			context.write(outputkey, NullWritable.get());
		}
	}
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://master:9000");
			
			Job job;
			job=Job.getInstance(conf, "MenAndWomenSales");
			job.setJarByClass(MenAndWomenSales.class);
			BasicConfigurator.configure();
			
			//设置专属的map和reduce
			job.setMapperClass(MenAndWomenSalesMapper.class);
			job.setReducerClass(MenAndWomenSalesReducer.class);
			//设置map的输出类型
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//设置reduce的输出类型			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			//设置文件的输入和输出位置
			FileInputFormat.addInputPath(job, new Path("/SHOP_LOG"));
			Path outputPath = new Path("/Test/MenAndWomenSales");
			//如果输出路径已存在，先删除
			FileSystem.get(conf).delete(outputPath,true);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			System.exit(job.waitForCompletion(true)?0:1);
			
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
}
