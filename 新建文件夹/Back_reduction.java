import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

    public class Back_reduction {
	    private int reduction_attr_list[];//约简之后的属性列表，‘1’代表该属性被选择，‘0’代表该属性没有被选择
	    private int attr_num;//属性个数
	    private int object_num;//样本
	    private int intersection_relation[][][];//每个属性下样本之间的相交关系
	    private int equ_class[][];//等价类
	    private float n_threshold;//邻域阈值
	    private String datafilename;//原始数据文件地址
	    private Read_info relations;//原始数据信息
	    private String rank;//属性排序类别
	    public Back_reduction(String file,float threshold,Read_info relation,String rank1){
		    try{
		        this.datafilename=file;
		        this.n_threshold=threshold;		
		        this.relations=relation;
		        this.attr_num=relations.getAttr_num();
		        this.object_num=relations.getObject_num();
		        this.intersection_relation=relations.getIntersection_relation();
		        this.equ_class=relations.getEqu_class();
		        this.rank=rank1;
		    }
		    catch(Exception e){
	    	    e.printStackTrace();
		    }
	    }
	    public void setRank(String rank1){
	    	this.rank=rank1;
	    }
	    //依据每个属性下正域中样本数量计算基因重要度，读取相交关系文件，传入core_intersection_neighborhood and intersection_relation and equ_class
	    public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		    private int object_num_p;//样本个数
		    private int attr_num_p;//属性个数
		    private int [][] ec;//等价类
		    
		    @Override
		    protected void setup(Context context) throws IOException{
			    Configuration conf=context.getConfiguration();
			    object_num_p=Integer.parseInt(conf.get("object_num_p"));
			    attr_num_p=Integer.parseInt(conf.get("attr_num_p"));
			    ec=new int[2][object_num_p];
				
			    //从传入的参数中获得ec
			    for(int n=0;n<2;n++){
				    String name=new String();
				    name=name+n;
				    String s=new String();
				    s=conf.get(name);
				    for(int i=0;i<object_num_p;i++){
				    	if(s.charAt(i)=='1')
					        ec[n][i]=1;
				    	else
				    		ec[n][i]=0;
				    }
			    }	
		    }
		
		    @Override
	        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			    String line = value.toString();
			    String split[]=line.split("\t");
			    String split1[]=split[0].split(",");
			    int positive_region[][]=new int[2][attr_num_p];//各个属性条件下的正域
			    int positive_region_all[]=new int[attr_num_p];//各个属性条件下全部决策属性下的正域的并集
			    int []ir=new int[attr_num_p];//当前属性下样本之间的相交关系
			    int i,j;
			    i=Integer.parseInt(split1[0]);
			    j=Integer.parseInt(split1[1]);
			    String split2[]=split[1].split(" ");
			    int k;
			    for(int m=0;m<split2.length;m++){
			        String split3[]=split2[m].split(",");
			        k=Integer.parseInt(split3[0]);
			        ir[k]=Integer.parseInt(split3[1]);
			        
			        //使用蕴含计算正域
			        for(int n=0;n<2;n++){
			            if(ir[k]==0){
						    positive_region[n][k]=1;
					    }
						else{
						    if(ec[n][j]==1){
						         positive_region[n][k]=1;
						    }
						    else{
						        positive_region[n][k]=0;
						    	break;
						    }
					    }
			        }
			        
			        //进行两个类别的正域合并
			        if((positive_region[0][k]==1)||(positive_region[1][k]==1)){
		                positive_region_all[k]=1;
	                }
		            else{
			            positive_region_all[k]=0;
		            }
			        
				    context.write(new Text(i+","+k), new IntWritable(positive_region_all[k]));//输出到中间结果以样本i,属性k为键值，（以样本j为前提进行蕴含计算）在k属性下样本i在正域中value为1，不在为0
			    }	    
		    }
	    } 

	    public static class FirstReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable>{	
	    	@Override
	        protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
	    		IntWritable temp=new IntWritable();
	    		int flag=1;
	    		
	    		//结果合并，样本i在以所有其他样本为前提下进行蕴含计算都为真，则样本i在属性k下的正域中
	    	    for(IntWritable t : values){
	   		        temp=t;
	   		        if(temp.toString().equals("0")){
	   		        	flag=0;
	   		        }
	            }
	    	    String s=key.toString();
	    	    String split[]=s.split(","); 
	    	    int i=Integer.parseInt(split[0]);
	    	    int k=Integer.parseInt(split[1]);
	    	    if(flag==1){
	    	    	context.write(new IntWritable(k),new IntWritable(i));//输出到hdfs中以（属性k,在该属性的正域中的样本i）
	    	    }    
	        }
	    }     
	    
	    //对基因进行依据信息熵的重要度计算，读取微阵列数据文件（data.txt），传入core_intersection_neighborhood and intersection_relation and equ_class
	    public static class FirstMapperInformationRank extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
		    private int object_num_p;//样本个数
		    private int [][] ec;//等价类
		    
		    @Override
		    protected void setup(Context context) throws IOException{
			    Configuration conf=context.getConfiguration();
			    object_num_p=Integer.parseInt(conf.get("object_num_p"));
			    ec=new int[2][object_num_p];

				
			    //从传入的参数中获得ec
			    for(int n=0;n<2;n++){
				    String name=new String();
				    name=name+n;
				    String s=new String();
				    s=conf.get(name);
				    for(int i=0;i<object_num_p;i++){
				    	if(s.charAt(i)=='1')
					        ec[n][i]=1;
				    	else
				    		ec[n][i]=0;
				    }
			    }	
		    }
		
		    @Override
	        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			    String line = value.toString();
			    String split[]=line.split(" ");
			    int id=Integer.parseInt(split[0]);
			    String split1[]=split[1].split(",");
			    ArrayList<Integer> small=new ArrayList<Integer>();
			    ArrayList<Integer> big=new ArrayList<Integer>();
			    
			    //对在每个属相下的样本划分类别，小于0.5划分到small类，大于0.5划分到big类
			    for(int i=0;i<object_num_p;i++){
			    	if(Double.parseDouble(split1[i])<=0.5){
			    		small.add(i);
			    	}
			    	else{
			    		big.add(i);
			    	}
			    }
			    
			    double small_n1=0.0;//small类中属于类别1的样本数量
			    double small_n2=0.0;//small类中属于类别2的样本数量
			    double big_n1=0.0;//big类中属于类别1的样本数量
			    double big_n2=0.0;//big类中属于类别2的样本数量
			    double I=0.0;//信息熵
			    
			    //在small类和big类中计算各个类别对应的样本数量
			    for(int i=0;i<small.size();i++){
			    	if(ec[0][small.get(i)]==1){
	                    small_n1=small_n1+1;
			    	}
			    	else{
			    		small_n2=small_n2+1;
			    	}
			    }
			    
                for(int i=0;i<big.size();i++){
                	if(ec[0][big.get(i)]==1){
	                    big_n1=big_n1+1;
			    	}
			    	else{
			    		big_n2=big_n2+1;
			    	}
			    }
                
                //计算信息熵
                I=(((double)small.size())/object_num_p)*(-(small_n1/small.size())*(Math.log(small_n1/small.size())/Math.log(2.0))-(small_n2/small.size())*(Math.log(small_n2/small.size())/Math.log(2.0)))+(((double)big.size())/object_num_p)*(-(big_n1/big.size())*(Math.log(big_n1/big.size())/Math.log(2.0))-(big_n2/big.size())*(Math.log(big_n2/big.size())/Math.log(2.0)));
                context.write(new IntWritable(id), new DoubleWritable(I));//输出到中间结果（属性标号，对应的信息熵）
		    }
	    } 

	    public static class FirstReducerInformationRank extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{	
	    	@Override
	        protected void reduce(IntWritable key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {
	            DoubleWritable temp=new DoubleWritable();
	    		for(DoubleWritable t : values){   
	    			temp=t;
	    		}
	    		context.write(key,temp);  
	        }
	    } 
	    
	  //依据秩和检测进行属性排序
	    public int[] wRank() throws Exception{
	    	int w_Rank[]=new int[this.attr_num];
	    	RankSumSelection r=new RankSumSelection(this.attr_num,this.datafilename);
	    	r.run();
	    	ArrayList<GenePValue> top_gene=r.gettop();
	    	for(int j=0;j<top_gene.size();j++){
				GenePValue tempgene=top_gene.get(j);
				int index=tempgene.getgenenum();
				w_Rank[j]=index;
			}
	    	return w_Rank;
	    }
	    
	    //对基因进行依据秩和检测的重要度计算，读取微阵列数据文件（data.txt），传入core_intersection_neighborhood and intersection_relation and equ_class
	    public static class FirstMapperWRank extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
		    private int object_num_p;//样本个数
		    private int [][] ec;//等价类
		    
		    @Override
		    protected void setup(Context context) throws IOException{
			    Configuration conf=context.getConfiguration();
			    object_num_p=Integer.parseInt(conf.get("object_num_p"));
			    ec=new int[2][object_num_p];

				
			    //从传入的参数中获得ec
			    for(int n=0;n<2;n++){
				    String name=new String();
				    name=name+n;
				    String s=new String();
				    s=conf.get(name);
				    for(int i=0;i<object_num_p;i++){
				    	if(s.charAt(i)=='1')
					        ec[n][i]=1;
				    	else
				    		ec[n][i]=0;
				    }
			    }	
		    }
		
		    @Override
	        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			    String line = value.toString();
			    String split[]=line.split(" ");
			    int id=Integer.parseInt(split[0]);
			    String split1[]=split[1].split(",");
			    GenePValue tempgene=new GenePValue();
			    for(int i=0;i<split1.length;i++){
			        if(ec[0][i]==1){
			            tempgene.add(1, split1[i]);
			        }
			        else{
			        	tempgene.add(2, split1[i]);
			        }
			    }
			    tempgene.computepvalue();
			    context.write(new IntWritable(id), new DoubleWritable(tempgene.getpvalue()));
		    }
	    } 

	    public static class FirstReducerWRank extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>{	
	    	@Override
	        protected void reduce(IntWritable key,Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException {
	    		DoubleWritable temp=new DoubleWritable();
		    	for(DoubleWritable t : values){   
		    		temp=t;
		    	}
		    	context.write(key,temp); 
	        }
	    } 
	    
	    //依据秩和检测进行属性排序
	    /*public int[] wRank() throws Exception{
	    	int w_Rank[]=new int[this.attr_num];
	    	RankSumSelection r=new RankSumSelection(this.attr_num,this.datafilename);
	    	r.run();
	    	ArrayList<GenePValue> top_gene=r.gettop();
	    	for(int j=0;j<top_gene.size();j++){
				GenePValue tempgene=top_gene.get(j);
				int index=tempgene.getgenenum();
				w_Rank[j]=index;
			}
	    	return w_Rank;
	    }*/
	    
	    //进行并行计算，传入core_intersection_neighborhood equ_class core_list参数
	    public void runParallel(String rank) throws Exception{
		    Configuration conf = new Configuration();
		    for(int n=0;n<2;n++){
			    String name=new String();
			    name=name+n;
			    StringBuilder s=new StringBuilder();
			    for(int i=0;i<object_num;i++){
				    s.append(equ_class[n][i]);
			    }
			    conf.set(name, s.toString());
		    }
		    
		    //对正域类型重要度进行并行计算
		    if(rank.equals("positive_region")){
		        conf.set("mapred.jar", "Back_reduction.jar"); 
		        conf.set("fs.default.name", "hdfs://localhost:9000");
		        conf.set("mapred.job.tracker", "localhost:9001");
		        conf.set("mapred.child.java.opts","-Xmx2048m");
		        conf.setInt("mapred.task.timeout", 36000000);
		        conf.setInt("object_num_p", object_num);
		        conf.setInt("attr_num_p", attr_num);
		        Job job = new Job(conf);
		        job.setJarByClass(Back_reduction.class);
		        job.setJobName("SigPositive_region");
		        job.setOutputKeyClass(IntWritable.class);
		        job.setOutputValueClass(IntWritable.class);
		        job.setMapOutputKeyClass(Text.class);
		        job.setMapOutputValueClass(IntWritable.class);
		        job.setMapperClass(Back_reduction.FirstMapper.class);
		        job.setReducerClass(Back_reduction.FirstReducer.class);
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
		        String inputPath="hdfs://localhost:9000/hadoop/RoughSet/output/"+n_threshold+"/";
		        String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output2/"+n_threshold+"/";
		        FileInputFormat.addInputPath(job, new Path(inputPath));
		        FileOutputFormat.setOutputPath(job, new Path(outputPath));
		        job.waitForCompletion(true);
		    }
		    
		    //对信息熵类型重要度记性并行计算
		    if(rank.equals("Information")){
		        conf.set("mapred.jar", "Back_reduction.jar"); 
		        conf.set("fs.default.name", "hdfs://localhost:9000");
		        conf.set("mapred.job.tracker", "localhost:9001");
		        conf.set("mapred.child.java.opts","-Xmx2048m");
		        conf.setInt("mapred.task.timeout", 36000000);
		        conf.setInt("object_num_p", object_num);
		        conf.setInt("attr_num_p", attr_num);
		        Job job = new Job(conf);
		        job.setJarByClass(Back_reduction.class);
		        job.setJobName("SigInformation");
		        job.setOutputKeyClass(IntWritable.class);
		        job.setOutputValueClass(DoubleWritable.class);
		        job.setMapOutputKeyClass(IntWritable.class);
		        job.setMapOutputValueClass(DoubleWritable.class);
		        job.setMapperClass(Back_reduction.FirstMapperInformationRank.class);
		        job.setReducerClass(Back_reduction.FirstReducerInformationRank.class);
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
		        String inputPath="hdfs://localhost:9000/hadoop/RoughSet/input/data.txt";
		        String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output3/"+n_threshold+"/";
		        FileInputFormat.addInputPath(job, new Path(inputPath));
		        FileOutputFormat.setOutputPath(job, new Path(outputPath));
		        job.waitForCompletion(true);
		    }
		    
		    //对秩和检测类型重要度记性并行计算
		    /*if(rank.equals("WRank")){
		        conf.set("mapred.jar", "Back_reduction.jar"); 
		        conf.set("fs.default.name", "hdfs://localhost:9000");
		        conf.set("mapred.job.tracker", "localhost:9001");
		        conf.set("mapred.child.java.opts","-Xmx2048m");
		        conf.setInt("mapred.task.timeout", 36000000);
		        conf.setInt("object_num_p", object_num);
		        conf.setInt("attr_num_p", attr_num);
		        Job job = new Job(conf);
		        job.setJarByClass(Back_reduction.class);
		        job.setJobName("SigWRank");
		        job.setOutputKeyClass(IntWritable.class);
		        job.setOutputValueClass(DoubleWritable.class);
		        job.setMapOutputKeyClass(IntWritable.class);
		        job.setMapOutputValueClass(DoubleWritable.class);
		        job.setMapperClass(Back_reduction.FirstMapperWRank.class);
		        job.setReducerClass(Back_reduction.FirstReducerWRank.class);
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
		        String inputPath="hdfs://localhost:9000/hadoop/RoughSet/input/data.txt";
		        String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output5/"+n_threshold+"/";
		        FileInputFormat.addInputPath(job, new Path(inputPath));
		        FileOutputFormat.setOutputPath(job, new Path(outputPath));
		        job.waitForCompletion(true);
		    }*/
	    }

	    //(正域)读取重要性文件，计算每个属性的重要度
	    public int[] getSelectAttr(String rank)throws Exception{
		    this.runParallel(rank);
		    System.out.println("开始读取reduce文件");
		    String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output2/"+n_threshold+"/part-r-00000";
		    int sig[]=new int[attr_num];
		    Configuration conf=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
			FSDataInputStream in=fs.open(new Path(outputPath));
			InputStreamReader ins=new InputStreamReader(in);
			BufferedReader bReader=new BufferedReader(ins);
		    String line=null;
		    while((line=bReader.readLine())!=null){
			    String split[]=line.split("\t");
			    int k=Integer.parseInt(split[0]);
			    sig[k]=sig[k]+1;
		    }
	        /*for(int k=0;k<attr_num;k++){
	            System.out.print(sig[k]+" ");
	        }*/
		    bReader.close();
		    System.out.println("获得属性重要度完成");
		    return sig;
	    }
	    
	    //(信息熵)读取重要性文件，计算每个属性的重要度
	    public double[] getSelectAttrInformation(String rank)throws Exception{
		    this.runParallel(rank);
		    System.out.println("开始读取reduce文件");
		    String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output3/"+n_threshold+"/part-r-00000";
		    double sig[]=new double[attr_num];
		    Configuration conf=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
			FSDataInputStream in=fs.open(new Path(outputPath));
			InputStreamReader ins=new InputStreamReader(in);
			BufferedReader bReader=new BufferedReader(ins);
		    String line=null;
		    while((line=bReader.readLine())!=null){
			    String split[]=line.split("\t");
			    int k=Integer.parseInt(split[0]);
			    sig[k]=Double.parseDouble(split[1]);
		    }
	        /*for(int k=0;k<attr_num;k++){
	            System.out.print(sig[k]+" ");
	        }*/
		    bReader.close();
		    System.out.println("获得属性重要度完成");
		    return sig;
	    }
	    
	    //(P_value)读取重要性文件，计算每个属性的重要度
	    public double[] getSelectWRank(String rank)throws Exception{
		    this.runParallel(rank);
		    System.out.println("开始读取reduce文件");
		    String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output5/"+n_threshold+"/part-r-00000";
		    double sig[]=new double[attr_num];
		    Configuration conf=new Configuration();
			FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
			FSDataInputStream in=fs.open(new Path(outputPath));
			InputStreamReader ins=new InputStreamReader(in);
			BufferedReader bReader=new BufferedReader(ins);
		    String line=null;
		    while((line=bReader.readLine())!=null){
			    String split[]=line.split("\t");
			    int k=Integer.parseInt(split[0]);
			    sig[k]=Double.parseDouble(split[1]);
		    }
		    bReader.close();
		    System.out.println("获得属性重要度完成");
		    return sig;
	    }
	    
	    //（int数组）对重要性数组的标号进行排序
	    public int[] selectSort(int[] args){//选择排序算法  
	        int sort[]=new int[args.length]; 
	        for(int i=0;i<args.length;i++){
	            sort[i]=i;
	        }
	        for (int i=0;i<args.length-1;i++ ){   
	            int min=i;   
	            for (int j=i+1;j<args.length;j++ ){   
	                if (args[sort[min]]>args[sort[j]]){   
	                    min=j;   
	                }   
	            }   
	            if (min!=i){  
	                int temp=sort[i];  
	                sort[i]=sort[min];  
	                sort[min]=temp;          
	            }  
	        }  
	        return sort;  
	    } 
	    
	    //(double数组)对重要性数组的标号进行排序
	    public int[] selectSortDouble(double[] args){//选择排序算法  
	        int sort[]=new int[args.length]; 
	        for(int i=0;i<args.length;i++){
	            sort[i]=i;
	        }
	        for (int i=0;i<args.length-1;i++ ){   
	            int min=i;   
	            for (int j=i+1;j<args.length ;j++ ){   
	                if (args[sort[min]]>args[sort[j]]){   
	                    min=j;   
	                }   
	            }   
	            if (min!=i){  
	                int temp=sort[i];  
	                sort[i]=sort[min];  
	                sort[min]=temp;          
	            }  
	        }  
	        return sort;  
	    } 
	    
	    //进行属性约简，返回约简后的属性列表
	    public int [] getReduction_attr_list(String rank) throws Exception{
		    reduction_attr_list=new int[attr_num];
		    for(int i=0;i<attr_num;i++){
		    	reduction_attr_list[i]=1;
		    }
		    
		    //计算全属性下的正域
		    int p_intersection_neighborhood[][]=new int [object_num][object_num];
		    Intersection_neighborhood neighborhood=new Intersection_neighborhood(reduction_attr_list,relations,"hdfs://localhost:9000/hadoop/RoughSet/output1/"+n_threshold+"/ar_all/");
		    p_intersection_neighborhood=neighborhood.getIntersectionNeighborhood();
		    Positive_region positive_region_all=new Positive_region(relations,p_intersection_neighborhood);
		    positive_region_all.positive_region();
		    int []positive_region=new int[object_num];
		    positive_region=positive_region_all.getPositive_region_all();
		    int p_num=0;
		    
		    for(int i=0;i<object_num;i++){
		    	if(positive_region[i]==1){
		    		p_num++;
		    	}
		    }
		    System.out.println("全属性下正域包含的样本数量"+p_num);
		
		    int add_sort[]=new int [attr_num];
		    for(int i=0;i<attr_num;i++){
		    	add_sort[i]=-1;
		    }
		    //正域类型属性约简
		    if(rank.equals("positive_region")){
		        int flag=0;
		        int []attr_sig=new int[attr_num];
	            int []sort=new int[attr_num];
	            System.out.println("开始进行属性重要度计算");
	            attr_sig=getSelectAttr(this.rank);
	      
	            System.out.println("完成属性重要度计算");
	            System.out.println("开始对重要性数组进行排序");
	            sort=selectSort(attr_sig);
		        System.out.println("对重要性数组进行排序完成");
		    
		        //直到当前正域与全属性下的正域相同则停止添加属性
		        System.out.println("开始进行属性添加");
		        Positive_region n_positive_region=new Positive_region(relations,p_intersection_neighborhood);
		        int num=0;
		        int [][]neighborhood_p=new int [object_num][object_num];
		        for(int i=0;i<object_num;i++){
		    	    for(int j=0;j<object_num;j++){
		    	    	neighborhood_p[i][j]=1;
		    	    }
		        }
		   
		        for(int i=0;i<attr_num;i++){
		        	reduction_attr_list[i]=0;
		        }
		        int p_max;
		        int n=attr_num-1;
		        int n_p[][]=new int[object_num][object_num];
		        int p_r[]=new int[object_num];
		        while(flag==0&&num<100){     
	                p_max=sort[n];
	                System.out.println("p_max="+p_max);
	                n=n-1;
	                add_sort[num]=p_max;
		            num++;
		            System.out.println("添加第"+num+"个属性");
		            reduction_attr_list[p_max]=1;
		            n_p=neighborhood_p;
		   
		            for(int j=0;j<object_num;j++){
	                    for(int k=0;k<object_num;k++){
	                    	if(neighborhood_p[j][k]==1&&intersection_relation[j][k][p_max]==1)
	                    	    neighborhood_p[j][k]=1;
	                    	else
	                	    	neighborhood_p[j][k]=0;
	    		        }
	    	        }
		            n_positive_region.setIntersection_neighborhood(neighborhood_p);
		            n_positive_region.positive_region();
		            int []positive_region_n=new int[object_num];
		            positive_region_n=n_positive_region.getPositive_region_all();
		            int n1=0;
		            for(int m=0;m<object_num;m++){
		            	if(p_r[m]==1){
		            		n1=n1+1;
		            	}
		            }
		            int n2=n_positive_region.getNum();
		            if(n2>=n1){
		                p_r=positive_region_n;
		                n_p=neighborhood_p;
		                for(int i=0;i<object_num;i++){
		                    System.out.print(positive_region_n[i]+" ");
		                }
		                System.out.println();
		                System.out.println("计算第"+num+"次正域");
		                for(int i=0;i<object_num;i++){
		                    if(positive_region_n[i]!=positive_region[i]){//比较当前正域与全属性下的正域是否相同
		    	                flag=0;
		    		            break;
		                    }
		                    else{
		    		            flag=1;
		                    }
		                } 
		            }
		            else{
		            	reduction_attr_list[p_max]=0;
		            	neighborhood_p=n_p;
		            	num--;
		            }
		        }
	            System.out.println("共添加"+num+"个属性");
	            
		    }
		    
		    //信息熵类型属性约简
		    if(rank.equals("Information")){
		        int flag=0;
		        int []sort=new int[attr_num];
		        double []attr_sig=new double[attr_num];
	            
	            System.out.println("得到属性重要度");
	            attr_sig=getSelectAttrInformation(this.rank);
	            System.out.println("得到属性重要度");
	            
	            System.out.println("开始对重要性数组进行排序");
	            sort=selectSortDouble(attr_sig);
		        System.out.println("对重要性数组进行排序完成");
		    
		        //直到当前正域与全属性下的正域相同则停止添加属性
		        System.out.println("开始进行属性添加");
		        Positive_region n_positive_region=new Positive_region(relations,p_intersection_neighborhood);
		        int num=0;
		        int [][]neighborhood_p=new int [object_num][object_num];
	
		        for(int i=0;i<object_num;i++){
		    	    for(int j=0;j<object_num;j++){
		    	    	neighborhood_p[i][j]=1;
		    	    }
		        }
		 
		        for(int i=0;i<attr_num;i++){
		        	reduction_attr_list[i]=0;
		        }
		        int p_min;
		        int n=0;
		        while(flag==0&&num<100){     
	                p_min=sort[n];
	                System.out.println("p_min="+p_min);
	                n=n+1;
	                add_sort[num]=p_min;
		            num++;
		            System.out.println("添加第"+num+"个属性");
		            reduction_attr_list[p_min]=1; 
		            for(int j=0;j<object_num;j++){
	                    for(int k=0;k<object_num;k++){
	                    	neighborhood_p[j][k]=neighborhood_p[j][k]&intersection_relation[j][k][p_min];
	    		        }
	    	        }
		            n_positive_region.setIntersection_neighborhood(neighborhood_p);
		            n_positive_region.positive_region();
		            int []positive_region_n=new int[object_num];
		            positive_region_n=n_positive_region.getPositive_region_all();
		            for(int i=0;i<object_num;i++){
		                System.out.print(positive_region_n[i]+" ");
		            }
		            System.out.println();
		            System.out.println("计算第"+num+"次正域");
		            for(int i=0;i<object_num;i++){
		                if(positive_region_n[i]!=positive_region[i]){//比较当前正域与全属性下的正域是否相同
		    	            flag=0;
		    		        break;
		                }
		                else{
		    		        flag=1;
		                }
		            } 
		        }
	            System.out.println("共添加"+num+"个属性");
		    }
		    if(rank.equals("WRank")){
		        int flag=0;
		        int []sort=new int[attr_num];
	            System.out.println("开始进行属性重要度计算");
	            sort=wRank();
	            System.out.println("完成属性重要度计算");
	         
		        //直到当前正域与全属性下的正域相同则停止添加属性
		        System.out.println("开始进行属性添加");
		        Positive_region n_positive_region=new Positive_region(relations,p_intersection_neighborhood);
		        int num=0;
		        int [][]neighborhood_p=new int [object_num][object_num];
		       
		        for(int i=0;i<object_num;i++){
		    	    for(int j=0;j<object_num;j++){
		    	    	neighborhood_p[i][j]=1;
		    	    }
		        }
		        
		        for(int i=0;i<attr_num;i++){
		        	reduction_attr_list[i]=0;
		        }
		        int p_min;
		        int n=0;
		        while(flag==0&&num<100){     
	                p_min=sort[n];
	                System.out.println("p_min="+p_min);
	                n=n+1;
	                add_sort[num]=p_min;
		            num++;
		            System.out.println("添加第"+num+"个属性");
		            reduction_attr_list[p_min]=1; 
		            for(int j=0;j<object_num;j++){
	                    for(int k=0;k<object_num;k++){
	                    	neighborhood_p[j][k]=neighborhood_p[j][k]&intersection_relation[j][k][p_min];
	    		        }
	    	        }
		           
		            n_positive_region.setIntersection_neighborhood(neighborhood_p);
		            n_positive_region.positive_region();
		            int []positive_region_n=new int[object_num];
		            positive_region_n=n_positive_region.getPositive_region_all();
		            for(int i=0;i<object_num;i++){
		                System.out.print(positive_region_n[i]+" ");
		            }
		            System.out.println();
		            System.out.println("计算第"+num+"次正域");
		            for(int i=0;i<object_num;i++){
		                if(positive_region_n[i]!=positive_region[i]){//比较当前正域与全属性下的正域是否相同
		    	            flag=0;
		    		        break;
		                }
		                else{
		    		        flag=1;
		                }
		            } 
		        }
	            System.out.println("共添加"+num+"个属性");    
		    }
		    
		    
		    //进行属性后向删除
		    int in1[][]=new int[object_num][object_num];
		    Intersection_neighborhood i_r1=new Intersection_neighborhood(reduction_attr_list,this.relations,"hdfs://localhost:9000/hadoop/RoughSet/output1/"+n_threshold+"/"+this.rank+"/all/");
		    in1=i_r1.getIntersectionNeighborhood();
		    Positive_region p_r1=new Positive_region(this.relations,in1);
		    int pr1[]=p_r1.getPositive_region_all();
		    int flag=0;
		    for(int i=attr_num-1;i>=0;i--){
		    	if(add_sort[i]!=-1){
		    		if(flag==1){
		    		    reduction_attr_list[add_sort[i]]=0;
		    		    int in[][]=new int[object_num][object_num];
		    		    Intersection_neighborhood i_r=new Intersection_neighborhood(reduction_attr_list,this.relations,"hdfs://localhost:9000/hadoop/RoughSet/output1/"+n_threshold+"/"+this.rank+"/"+i+"/");
		    		    in=i_r.getIntersectionNeighborhood();
		    		    Positive_region p_r=new Positive_region(this.relations,in);
		    		    int pr[]=p_r.getPositive_region_all();
		    		    int j;
		    		    for(j=0;j<object_num;j++){
		    			    if(pr1[j]!=pr[j]){
		    				    reduction_attr_list[add_sort[i]]=1;
		    				    System.out.println("属性"+add_sort[i]+"不可以删除");
		    				    break;
		    			    }
		    		    }
		    		    if(j==object_num){
		    		        System.out.println("属性"+add_sort[i]+"可以删除");
		    		    }
		    	    }
		    		flag=1;
		    	}
		    }
		    
		    int n=0;
		    for(int i=0;i<attr_num;i++){
		    	if(reduction_attr_list[i]==1){
		    		n=n+1;
		    	}
		    }
		    System.out.println("共"+n+"个属性");
		    return reduction_attr_list;
	    }
	    
	    //输出约简后的属性对应的数据信息，生成训练数据集
	    public void print_red_data() throws Exception{
	        System.out.println("开始进行文件输出");
		    //构建存放文件的文件夹
		    File in=new File(this.datafilename);
		    String path=in.getParent();
		    path=path+"/Red_data";
		    File r=new File(path);
		    if(!r.exists()){
			      r.mkdirs();
		    }
		    
		    //读文件流
		    FileInputStream ins=new FileInputStream(this.datafilename);
		    InputStreamReader inReader=new InputStreamReader(ins);
		    BufferedReader bReader=new BufferedReader(inReader);
		    String rline=null;
		    
		    //新文件命名
		    System.out.println("写"+n_threshold+"文件");
		    String newname=null;
		    if(rank.equals("positive_region")){
		    	 newname="/"+n_threshold+"P.arff";
		    }
		    if(rank.equals("Information")){
		         newname="/"+n_threshold+"I.arff";
		    }
		    if(rank.equals("WRank")){
		    	 newname="/"+n_threshold+"W.arff";
		    }
			
		    //写文件流
		    File fout = new File(path+newname);	
		    BufferedWriter bwriter = new BufferedWriter(new FileWriter(fout,false));	
		    
		    //按行读文件并处理产生新文件
		    ArrayList<String> attr=new ArrayList<String>();
		    int flag=0;
		    while((rline=bReader.readLine())!=null){
			    if(rline.substring(0,5).toUpperCase().equals("@DATA")){
				    flag=1;
				    break;
			    }
			    else if(rline.substring(0,10).toUpperCase().equals("@ATTRIBUTE")){
				    attr.add(rline);
				}
				else{
			        bwriter.write(rline+"\r\n");
				}
			}
		    
			if(flag==1){//读到数据标志而退出时的情况
				for(int j=0;j<attr_num;j++){//输出选择后的基因描述
				    if(reduction_attr_list[j]==1)
					    bwriter.write(attr.get(j)+"\r\n");
				    }
				    bwriter.write(attr.get(attr.size()-1)+"\r\n");//输出类别描述
				    bwriter.write(rline+"\r\n");//输出@DATA
				    while((rline=bReader.readLine())!=null){//调整数据部分
					    String outdata=null;
					    String split[]=rline.split(",");
					    for(int j=0;j<attr_num;j++){//输出选择后的基因数据
					        if(reduction_attr_list[j]==1){
						        if(outdata==null){
							        outdata=split[j];
						        }
						        else{
							        outdata+=","+split[j];
						        }
						    }
					    }
					    outdata+=","+split[split.length-1];
					    bwriter.write(outdata+"\r\n");
			      }
			  }
			  bwriter.close();
			  bReader.close();
			  inReader.close();
			  ins.close();        
	    } 
	    
	    public void run(){
		    try{
		        this.getReduction_attr_list(this.rank);
	            System.out.println("数据约简成功");
		        this.print_red_data();
	            System.out.println("生成训练集成功");         
		    }
		    catch(Exception e){
			    e.printStackTrace();
		    }
	    }
	}
