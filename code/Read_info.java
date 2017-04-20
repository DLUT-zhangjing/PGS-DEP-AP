import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class Read_info {
	private String datafile;//用于约简的微阵列数据文件
	private float n_threshold;//邻域阈值
	private int attr_num;//属性个数（不包含决策属性）
	private int object_num;//样本个数
	private ArrayList<String> attr_name_list=new ArrayList<String>();//属性名列表（不包含决策属性）	
	private ArrayList<String> dec_classname_list=new ArrayList<String>();//类别名列表
	private double[][] max_min_values;//记录每个属性的最大值和最小值，min存在max_min_values[][0],max存在max_min_values[][1]
	private int [][] equ_class;//依据决策属性划分的等价类
	private String inputPath=new String();//mapreduce过程的输入文件的地址
	private String outputPath=new String();//mapreduce过程的输出文件的地址
	private int intersectionRelation [][][];//每个条件属性下样本之间的相交关系
	public Read_info(String datafilename,float threshold){
		this.datafile=datafilename;
		this.n_threshold=threshold;
	}
	
	public int getAttr_num() {
		return attr_num;
	}

	public int getObject_num() {
		return object_num;
	}

    public ArrayList<String> getAttr_name_list(){
    	return this.attr_name_list;
    }
    
    public ArrayList<String> getDec_classname_list(){
    	return this.dec_classname_list;
    }
    public int[][] getEqu_class(){
    	return this.equ_class;
    }
    public int[][][] getIntersection_relation(){
    	return this.intersectionRelation;
    }
    public String getDatafile(){
    	return this.datafile;
    }
    public float getThreshold(){
    	return this.n_threshold;
    }
    
    //读取第i个样本的信息
	public String readdata(String file,int object_id) throws Exception{
		String out=null;
		FileInputStream in=new FileInputStream(file);
		InputStreamReader inReader=new InputStreamReader(in);
		BufferedReader bReader=new BufferedReader(inReader);
		String line=null;
		int flag=0;
		while((line=bReader.readLine())!=null){//找到@DATA行
			if(line!="" && line.substring(0, 5).toUpperCase().equals("@DATA")){
				flag=1;
				break;
			}
		}
		int i=-1;
		if(flag==1){//当读取到"@DATA"退出上一层循环，说明有数据，读取具体数据
			do{
				i++;
				line=bReader.readLine();
			}while(line!=null && i!=object_id);
			out=line;
		}
		bReader.close();
		inReader.close();
		in.close();
		return out;
	}
	
	/**
	 * 读文件中数据对象个数，属性个数，并将属性名和类别名分别添加到对应的列表中，找到每个属性的最大值和最小值
	 * @throws Exception
	 */
	public void read_info(String file)throws Exception{
        //清空attr_name_list和dec_classname_list
        Iterator<String> it = attr_name_list.iterator();  
        for(;it.hasNext();) {  
            it.next();  
            it.remove(); 
        }
        Iterator<String> it1 = dec_classname_list.iterator();  
        for(;it1.hasNext();) {  
            it1.next();  
            it1.remove(); 
        }
		FileInputStream in=new FileInputStream(file);
		InputStreamReader inReader=new InputStreamReader(in);
		BufferedReader bReader=new BufferedReader(inReader);
		String line=null;
		int flag=0;
		while((line=bReader.readLine())!=null){
			if(line!=""){
				if(line.substring(0, 5).toUpperCase().equals("@DATA")){//找到@DATA行
					flag=1;
					break;
				}
				else if(line.substring(0, 10).toUpperCase().equals("@ATTRIBUTE")){
					String split[]=line.split(" ");
					if(split.length==3 && !split[1].toUpperCase().equals("CLASS")){//添加属性列表
						attr_name_list.add(split[1]);
					}
					else if(split[1].toUpperCase().equals("CLASS")){//添加类别列表
						String temp=line.substring(18,line.length()-1);
						String split1[]=temp.split(",");
						for(int i=0;i<split1.length;i++){
							split1[i]=split1[i].trim();
						}
						for(int i=0;i<split1.length;i++){
							dec_classname_list.add(split1[i]);
						}
					}
				}
			}
		}
		this.attr_num=attr_name_list.size();//设置属性个数
		if(flag==1){//读取数据对象个数及每个属性下的最大值和最小值
			int i=0;
			max_min_values=new double[attr_num][2];
			for(int j=0;j<attr_num;j++){//初始化最大值和最小值
				max_min_values[j][0]=Double.MAX_VALUE;
				max_min_values[j][1]=-Double.MAX_VALUE;
			}
			while((line=bReader.readLine())!=null){
				String split[]=line.split(",");
				for(int j=0;j<split.length-1;j++){
					double temp=Double.parseDouble(split[j]);
					if(max_min_values[j][0]>temp){
						max_min_values[j][0]=temp;
					}
					if(max_min_values[j][1]<temp){
						max_min_values[j][1]=temp;
					}
				}
				i++;
			}
			object_num=i;
		}
		bReader.close();
		inReader.close();
		in.close();
	}
	
	//将原始数据标准化，并以“属性标号 样本0在此属性下的表达值，样本1在此属性下的表达值，...”形式输出到文件中
	public void write_attr() throws Exception{ 
        this.read_info(this.datafile);
		//属性值是0~1标准化后的数据
		double data[][]=new double[attr_num][object_num];
		for(int i=0;i<object_num;i++){
		    String i_string=this.readdata(this.datafile,i);
			String i_split[]=i_string.split(",");
			double i_data[]=new double[attr_num];
			for(int pi=0;pi<attr_num;pi++){
				i_data[pi]=Double.parseDouble(i_split[pi]);
				double di=max_min_values[pi][1]-max_min_values[pi][0];
				i_data[pi]=(i_data[pi]-max_min_values[pi][0])/di;//0~1标准化
				data[pi][i]=i_data[pi];
			}
		}
		//构建存放文件的文件夹
	    File in=new File(this.datafile);
	    String path=in.getParent();
		File r=new File(path);
		if(!r.exists()){
		    r.mkdirs();
		}
		//新文件命名
		System.out.println("写"+"data"+"文件");
		String newname="/"+"data"+".txt";
				
		//写文件流
		File fout = new File(path+newname);	
		BufferedWriter bwriter = new BufferedWriter(new FileWriter(fout,false));	
		for(int k=0;k<attr_num;k++){
			bwriter.write(k+" ");
			System.out.println("第"+k+"个属性");
		    int i;
			for(i=0;i<object_num-1;i++){
				bwriter.write(data[k][i]+",");
				
			}
			bwriter.write(data[k][i]+"\r\n");
		}
		bwriter.close();
	}  

	//获得决策条件下的等价类
    public int[][] equ_class() throws Exception{
	    //读取数据文件，获得等价类列表
    	this.read_info(datafile);
		equ_class=new int [2][object_num];
	    FileInputStream in=new FileInputStream(this.datafile);
		InputStreamReader inReader=new InputStreamReader(in);
		BufferedReader bReader=new BufferedReader(inReader);
		String line=null;
		int flag=0;
		while((line=bReader.readLine())!=null){//找到@DATA行
			if(line!="" && line.substring(0, 5).toUpperCase().equals("@DATA")){
				flag=1;
				break;
			}
		}
		if(flag==1){
			int i=0;
			while((line=bReader.readLine())!=null){//
				String split[]=line.split(",");
				for(int j=0;j<2;j++){
					if(split[split.length-1].equals(dec_classname_list.get(j))){
						equ_class[j][i]=1;
					}
					else
						equ_class[j][i]=0;
				}
				i++;
			}
		}
		bReader.close();
		inReader.close();
		in.close();
		return equ_class;
    }
    
    //并行计算样本之间的相交关系
    public static class FirstMapper extends Mapper<LongWritable, Text, Text, Text>{
	    private Text map_key=new Text();//key 设置成i,j
	    private Text map_value=new Text();//value 设置成k,1 或 k,0  表示第i个样本和第j个样本之间知否存在相交关系
	    private double threshold_p=0.0;
	    @Override
	    protected void setup(Context context) throws IOException{
	    	Configuration conf=context.getConfiguration();
	    	threshold_p=Double.parseDouble(conf.get("threshold"));
	    }
	    @Override
	    protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		    String line = value.toString();
		    String split[]=line.split(" ");
		    int k=Integer.parseInt(split[0]);
		    String split1[]=split[1].split(",");
		    int i,j;
		    
		    //由于对称性，只计算上三角的相交关系
		    for(i=0;i<split1.length;i++){
			    for(j=i+1;j<split1.length;j++){
				    if(Math.abs(Double.parseDouble(split1[i])-Double.parseDouble(split1[j]))<threshold_p){
					    map_key.set(i+","+j);
					    map_value.set(k+","+1);
					    context.write(map_key,map_value);
				    }
				    else{
					    map_key.set(i+","+j);
					    map_value.set(k+","+0);
					    context.write(map_key,map_value);
			       }
			   }
		   }
	    }   
	}
    
    //输出样本之间在各个属性下的相交关系
    public static class FirstReducer extends Reducer<Text,Text, Text, Text>{
    	//输出文件的数据形式key i,j value k,1 或k,0（样本i和样本j之间的相交关系）
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
	        String temp=new String();
	        for(Text t : values){
	        	if(temp.equals("")){
	        		temp=temp+t.toString();
	        	}
	        	else{
	                temp=temp+" "+t.toString();
	        	}
	        }
	        context.write(key, new Text(temp));
	    }
	}
    
    //运行mapreduce并行过程
	public void parallelIntesectionrRelation() throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.jar", "Read_info.jar"); 
		conf.set("fs.default.name", "hdfs://localhost:9000");
		conf.set("mapred.job.tracker", "localhost:9001");
		conf.setInt("mapred.task.timeout", 36000000);
	    conf.setFloat("threshold", n_threshold);
		Job job = new Job(conf);
		job.setJarByClass(Read_info.class);
		job.setJobName("IR");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(4);
		job.setMapperClass(Read_info.FirstMapper.class);
		job.setReducerClass(Read_info.FirstReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		inputPath="hdfs://localhost:9000/hadoop/RoughSet/input/data.txt";
		outputPath="hdfs://localhost:9000/hadoop/RoughSet/output/"+n_threshold+"/";
		FileInputFormat.setMinInputSplitSize(job, 6);
		FileInputFormat.setMaxInputSplitSize(job, 1048576);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
	}
	
	//读取mapreduce过程输出文件，获得相交关系矩阵
	public void intersection_relation() throws Exception{
		//this.parallelIntesectionrRelation();
		System.out.println("开始读取相交关系数据");
		for(int num=0;num<4;num++){
			System.out.println("开始读取part-r-0000"+num);
		    outputPath="hdfs://localhost:9000/hadoop/RoughSet/output/"+n_threshold+"/part-r-0000"+num;
		    Configuration conf=new Configuration();
		    FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
		    FSDataInputStream in=fs.open(new Path(outputPath));
		    InputStreamReader ins=new InputStreamReader(in);
		    BufferedReader bReader=new BufferedReader(ins);
		    intersectionRelation=new int[object_num][object_num][attr_num];
		    for(int i=0;i<object_num;i++){
		    	for(int k=0;k<attr_num;k++){
		    		intersectionRelation[i][i][k]=1;
		    	}
		    }
		    String line=null;
		    while((line=bReader.readLine())!=null){
			    String split[]=line.split("\t");
			    String split1[]=split[0].split(",");
			    int i,j;
			    i=Integer.parseInt(split1[0]);
			    j=Integer.parseInt(split1[1]);
			    String split2[]=split[1].split(" ");
			    for(int n=0;n<split2.length;n++){
				    String split3[]=split2[n].split(",");  
				    int k=Integer.parseInt(split3[0]);
				    intersectionRelation[i][j][k]=Integer.parseInt(split3[1]);
				    intersectionRelation[j][i][k]=Integer.parseInt(split3[1]);
			    }
		    }
		    bReader.close();
		}
	}
	
	public void run(){
		try{
			this.equ_class();
			this.intersection_relation();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	/*public static void main(String args[]){
		Read_info r=new Read_info("/home/hadoop/workspace/Rough/data/ArabidopsisDrought.arff",0.05f);
		r.run();
	}*/
}