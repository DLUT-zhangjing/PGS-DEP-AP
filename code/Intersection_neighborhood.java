import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class Intersection_neighborhood {
	private int attr_num;//属性个数（不包含决策属性）
	private int object_num;//样本个数
	private int intersection_neighborhood[][];//相交邻域
	private float n_threshold;//相交邻域阈值
	private int[] n_attr_list;//进行相交运算的属性列表，‘1’代表包含该属性，‘0’代表不包含该属性
	private Read_info relations;//数据集信息，包含每个属性下样本之间的相交关系，样本属性，类别等信息
	private String inputPath=new String();//mapreduce过程输入文件路径
	private String outputPath=new String();//mapreduce过程输出文件路径
	
	public Intersection_neighborhood(int [] attr_list,Read_info relation,String outputName){
		this.relations=relation;
		this.attr_num=relations.getAttr_num();
		this.n_attr_list=new int[attr_num];
		this.n_attr_list=attr_list;
		this.object_num=relations.getObject_num();
		this.n_threshold=relations.getThreshold();
		this.outputPath=outputName;
	}
	
	public int [][]getIntersectionNeighborhood()throws Exception{
		this.run();
		return this.intersection_neighborhood;
	}
	
	//并行计算map过程，用于样本相交关系的相交运算，产生相交邻域
	public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		String attrList=new String();
		 @Override
		 protected void setup(Context context) throws IOException{
			 Configuration conf=context.getConfiguration(); 
			 attrList=conf.get("attrList");
		 }
		 @Override
	     protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			 String line = value.toString();
		     String split[]=line.split("\t");
		     String split1[]=split[1].split(" ");
		     int relation=1;
		     for(int i=0;i<split1.length;i++){
		    	 String split2[]=split1[i].split(",");
		    	 int k=Integer.parseInt(split2[0]);
		    	 if(attrList.charAt(k)=='1'){
		    		 relation=relation&Integer.parseInt(split2[1]);
		    	 }
		     }
		     context.write(new Text(split[0]), new IntWritable(relation));   
		 }
	}
	
	public static class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
        	IntWritable temp=new IntWritable();
        	for(IntWritable t : values){
        		temp=t;
        	}
        	context.write(key, temp);
        }    
	}
	
	//运行mapreduce并行过程
	public void parallelIntersection_neighborhood() throws Exception{
		Configuration conf = new Configuration();
		conf.set("mapred.jar", "Intersection_neighborhood.jar"); 
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("mapred.job.tracker", "localhost:9001");
		int i;
		String s=new String();
		System.out.println(attr_num);
		for(i=0;i<attr_num;i++){
			s=s+n_attr_list[i];
		}
		conf.set("attrList",s);
		conf.setInt("mapred.task.timeout", 36000000);
		Job job = new Job(conf);
		job.setJarByClass(Intersection_neighborhood.class);
		job.setJobName("IN");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Intersection_neighborhood.FirstMapper.class);
		job.setReducerClass(Intersection_neighborhood.FirstReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		inputPath="hdfs://localhost:9000/hadoop/RoughSet/output/"+n_threshold+"/";
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
	//读取HDFS获得相交邻域
	public void intersection_neighborhood() throws Exception{
		if(!outputPath.contains("ar_all")){
		    this.parallelIntersection_neighborhood();
		}
		intersection_neighborhood=new int[object_num][object_num];
		for(int i=0;i<object_num;i++){
			intersection_neighborhood[i][i]=1;
		}
		System.out.println(outputPath);
		outputPath=outputPath+"part-r-00000";
		System.out.println(outputPath);
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
		FSDataInputStream in=fs.open(new Path(outputPath));
		InputStreamReader ins=new InputStreamReader(in);
		BufferedReader bReader=new BufferedReader(ins);
		String line=null;
		while((line=bReader.readLine())!=null){//找到@DATA行
			String split[]=line.split("\t");
			String split1[]=split[0].split(",");
			int intersectiongneighborhood=Integer.parseInt(split[1]);
			int i,j;
			i=Integer.parseInt(split1[0]);
			j=Integer.parseInt(split1[1]);
			intersection_neighborhood[i][j]=intersectiongneighborhood;
			intersection_neighborhood[j][i]=intersectiongneighborhood;
		}
		bReader.close();
	}
	
	public void run() throws Exception{
		this.intersection_neighborhood();	
	}
	/*public static void main(String args[]){
		try{
		Read_info r=new Read_info("/home/hadoop/workspace/Rough/data/ArabidopsisDrought.arff",0.05f);
		r.run();
		int [] a=new int[r.getAttr_num()];
		for(int i=0;i<r.getAttr_num();i++){
			a[i]=1;
		}
		Intersection_neighborhood in=new Intersection_neighborhood(a,r,"hdfs://localhost:9000/hadoop/RoughSet/output1/part-r-00000");
		in.run();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}*/
}
