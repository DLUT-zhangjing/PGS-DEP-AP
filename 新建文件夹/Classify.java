import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.functions.LibSVM;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.Normalize;

public class Classify {
	private String filepath;//用于分类的全部文件所在的文件路径
	private double percent;//抽样百分比，用于作为训练集
	private int num;//重复抽样次数
	private int obj_num;//样本个数
	private int classifier_num;//基分类器个数
	private char[] flag;//记录每个样本的隶属，0表示尚未分配，1表示属于训练集，2表示属于验证集，3表示属于测试集
	private ArrayList<String> dec_classname_list=new ArrayList<String>();//记录类别名
	private ArrayList<ArrayList<Integer>> class_obj_list=new ArrayList<ArrayList<Integer>>();//记录每类样本对应的对象id列表
	private int train_obj_num;//记录训练集样本个数
	private int validation_obj_num;//记录验证集样本个数
	private int test_obj_num;//记录测试机样本个数
	private ArrayList<String> old_label=new ArrayList<String>();//记录测试集原始类别标签
	private ArrayList<String> new_label=new ArrayList<String>();//记录测试集新分类标签
	private ArrayList<String> old_label1=new ArrayList<String>();//记录验证集原始类别标签
	private ArrayList<String> new_label1=new ArrayList<String>();//记录验证集新分类标签
	private double acc_list[];//记录基分类器的分类正确率
	private int num_list[];//记录每个基分类器对应的属性个数
	private double end_acc;//记录num次平均集成分类器分类正确率
	private int iter;//迭代次数
	private int Nthreshold;//进行集成的基分类器最大数量
	private double sim[][];//基分类器之间的相似性矩阵
	private double tp;//最终的tp值
	private double tn;//最终的tn值
	private double fn;//最终的fn值
	private double fp;//最终的fp值
	private double end_number;
	private int validation_result[][];//验证集分类结果
	private int test_result[][];//测试集分类结果
	private int test_class[];//测试集的样本类别
	private double K;
	public Classify(String path,double per,int n,int N){
		this.filepath=path;
		this.percent=per;
		this.num=n;
		this.Nthreshold=N;
	}
	
	public String getFilepath() {
		return filepath;
	}

	public double getPercent() {
		return percent;
	}

	public int getNum() {
		return num;
	}

	public int getObj_num() {
		return obj_num;
	}

	public int getClassifier_num(){
		return this.classifier_num;
	}

	public char[] getFlag() {
		return flag;
	}

	public ArrayList<String> getDec_classname_list() {
		return dec_classname_list;
	}

	public ArrayList<ArrayList<Integer>> getClass_obj_list() {
		return class_obj_list;
	}

	public int getTrain_obj_num() {
		return train_obj_num;
	}

	public ArrayList<String> getOld_label() {
		return old_label;
	}

	public ArrayList<String> getNew_label() {
		return new_label;
	}

	public double getEnd_acc() {
		return end_acc;
	}

	public double[] getAcc_list(){
		return this.acc_list;
	}
	
	//读列表中的一个文件，获取样本信息
	public void read_info()throws Exception{
		//ֻ只读取文件夹中的一个文件
		File file=new File(this.filepath);
		System.out.println(this.filepath);
		String[] file_list=file.list();
		this.classifier_num=file_list.length;
		this.acc_list=new double[file_list.length];//初始化分类正确率列表，记录基分类器的分类正确率
		this.num_list=new int[file_list.length];//初始化属性个数列表，记录基分类器对应的属性个数
		FileInputStream in=new FileInputStream(this.filepath+"/"+file_list[0]);
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
					if(split[1].toUpperCase().equals("CLASS")){//添加类别列表
						String temp=line.substring(18,line.length()-1);
						String split1[]=temp.split(",");
						for(int i=0;i<split1.length;i++){
							split1[i]=split1[i].trim();
							dec_classname_list.add(split1[i]);
						}
					}
				}
			}
		}
		//初始化类对象列表集
		for(int i=0;i<this.dec_classname_list.size();i++){
			ArrayList<Integer> temp=new ArrayList<Integer>();
			this.class_obj_list.add(temp);
		}
		if(flag==1){//读数据对象总个数及每个对象所属属性列表
			int i=0;
			while((line=bReader.readLine())!=null){
				String split[]=line.split(",");
				String obj_label=split[split.length-1].trim();
				for(int k=0;k<this.dec_classname_list.size();k++){
					String temp_class=this.dec_classname_list.get(k);
					if(obj_label.equals(temp_class)){
						ArrayList<Integer> temp_list=this.class_obj_list.get(k);
						temp_list.add(i);
						this.class_obj_list.set(k, temp_list);
					}
				}
				i++;
			}
			this.obj_num=i;
		}
		bReader.close();
		inReader.close();
		in.close();
	}
	
	//无放回抽样
	public void sampling(){
		//初始化
		int o_num=this.obj_num;
		flag=new char[o_num];
		for(int i=0;i<o_num;i++){
			flag[i]='0';
		}
		this.train_obj_num=0;
		this.validation_obj_num=0;
		//在每类对象中分别抽取
		for(int k=0;k<this.dec_classname_list.size();k++){
			ArrayList<Integer> temp_list=this.class_obj_list.get(k);//获得当前类的对象列表
			int class_obj_num=temp_list.size();
			int train_num=(int)(this.percent*class_obj_num);//训练集样本个数
			int validation_num=(int)((1-this.percent)*0.5*class_obj_num);
			this.train_obj_num=this.train_obj_num+train_num;
			this.validation_obj_num=validation_obj_num+validation_num;
			for(int m=0,n=0;m<class_obj_num || n<train_num;m++){
				int r=(int)(Math.random()*(class_obj_num-m));
				//System.out.println("r"+r);
				if(r<(train_num-n)){
					flag[temp_list.get(m)]='1';
					n++;
				}
				else{
					flag[temp_list.get(m)]='2';
				}
			}
			ArrayList<Integer> temp_list1=new ArrayList<Integer>();
			Iterator<Integer> it =temp_list1.iterator();  
	        for(;it.hasNext();) {  
	              it.next();  
	              it.remove(); 
	        }
			for(int i=0;i<class_obj_num;i++){
				if(flag[temp_list.get(i)]=='2'){
					temp_list1.add(temp_list.get(i));
				}
			}
			for(int i=0,j=0;i<temp_list1.size()||j<validation_num;i++){
				int r=(int)(Math.random()*(temp_list1.size()-i));
				if(r<(validation_num-j)){
					j++;
				}
				else{
					flag[temp_list1.get(i)]='3';
				}
			}	
		}
	}
	
	//使用训练集产生基分类器，并对验证集和测试集上中的样本进行分类
	public void classifier(){
		this.sampling();
		test_obj_num=this.obj_num-this.train_obj_num-this.validation_obj_num;
		System.out.println(test_obj_num);
		validation_result=new int[classifier_num][validation_obj_num];
		test_result=new int[classifier_num][test_obj_num];
		test_class=new int[test_obj_num];
		for(int m=0;m<classifier_num;m++)
		{     
			for(int n=0;n<validation_obj_num;n++){
			    this.validation_result[m][n]=0;
				
			}
		}
		for(int m=0;m<classifier_num;m++)
		{     
			for(int n=0;n<test_obj_num;n++){
			    this.test_result[m][n]=0;
			}
		}
		
		this.old_label.clear();
		this.new_label.clear();
		this.old_label1.clear();
		this.new_label1.clear();
		
		try{
			//遍历文件夹中的每一个数据文件
			File file=new File(this.filepath);
			String[] file_list=file.list();
			for(int i=0;i<file_list.length;i++){
				String path=this.filepath+"/"+file_list[i];
				System.out.println(file_list[i]);
				int n;
				String s=file_list[i];
				int l=s.indexOf(".");
				n=Integer.parseInt(s.substring(0, l))-1;
				System.out.println(n);
				//读取数据
				FileReader data=new FileReader(path);
				Instances m_instances = new Instances(data);
				m_instances.setClassIndex( m_instances.numAttributes() - 1 );//设置该实体的类别属性
				this.num_list[i]=m_instances.numAttributes()-1;//记录属性个数，决策属性除外
				System.out.println("读取数据成功");
				
				//进行数据标准化-1~1
				Normalize normalize=new Normalize();
				String options[]=weka.core.Utils.splitOptions("-S 2.0 -T -1.0");//设置参数
				normalize.setOptions(options);
				normalize.setInputFormat(m_instances);//设置输入文件
				Instances n_m_instances=Filter.useFilter(m_instances, normalize);//使用过滤器并生成新的数据
				//DataSink.write(this.filepath+"\\nor_"+file_list[i],n_m_instances);//输出标准化后的数据
				System.out.println("数据标准化成功");
				
				//进行数据分裂
				Instances train_instances = new Instances(n_m_instances);//记录训练集，初始化为全集
				Instances test_instances = new Instances(n_m_instances);//记录测试集，初始化全集
				Instances validation_instances=new Instances(n_m_instances);
				train_instances.setClassIndex( train_instances.numAttributes() - 1 );
				test_instances.setClassIndex( test_instances.numAttributes() - 1 );
				validation_instances.setClassIndex(validation_instances.numAttributes()-1);
				for(int j=this.obj_num-1;j>=0;j--){
					if(this.flag[j]=='1'){
						test_instances.delete(j);
						validation_instances.delete(j);
					}
					else if(this.flag[j]=='2'){
						train_instances.delete(j);
						test_instances.delete(j);
					}
					else if(this.flag[j]=='3'){
						train_instances.delete(j);
						validation_instances.delete(j);
					}
					else{
						System.out.println("数据分裂有误");
					}
				}
				System.out.println("数据集分裂成功");
				
				//用训练集，构建SVM分类器
				Classifier svm=new LibSVM();
				svm.buildClassifier(train_instances);
				
				//使用产生的基分类器对验证集中的样本进行分类
				for(int k=0;k<validation_instances.numInstances();k++){
					double t=validation_instances.instance(k).classValue();
					String temp_old_label1=validation_instances.classAttribute().value((int)t);
					if(i==0){
						this.old_label1.add(temp_old_label1);//添加旧标签
					}
					double class_l=svm.classifyInstance(validation_instances.instance(k));//分类
					String class_label1=validation_instances.classAttribute().value((int)class_l);//获得新标签
					if(class_label1.equals(temp_old_label1)){
						validation_result[n][k]=1;
					}
				}
				
				//测试，并记录对测试集样本的分类结果
				for(int k=0;k<test_instances.numInstances();k++){
					double t=test_instances.instance(k).classValue();
					String temp_old_label=test_instances.classAttribute().value((int)t);
					if(temp_old_label.equals("infected")){
						test_class[k]=1;
					}
					else{
						test_class[k]=0;
					}
					if(i==0){
						this.old_label.add(temp_old_label);//添加旧标签
					}
					
					double class_l=svm.classifyInstance(test_instances.instance(k));//分类
					String class_label=test_instances.classAttribute().value((int)class_l);//获得新标签
					
					//记录测试结果
					if(class_label.equals(temp_old_label)){
						test_result[n][k]=1;
						this.acc_list[n]=this.acc_list[n]+((double)1)/test_obj_num;
					}
			     }   
		     }
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	//输出每个基分类器在验证集上的分类结果，“1”代表分类正确，“0”代表分类错误
	public void print_validation_results() throws Exception{
		//构建存放文件的文件夹，用阈值命名
		File in=new File(this.filepath);
		String[] file_list=in.list();
	    String path=in.getParent();
		path=path+"/classificationResults";
	    File r=new File(path);
		if(!r.exists()){
			r.mkdirs();
		}
		//新文件命名
		System.out.println("写"+"ValidationResults"+"文件");
		String newname="/"+"ValidationResults.arff";
				
		//写文件流
		File fout = new File(path+newname);	
		BufferedWriter bwriter = new BufferedWriter(new FileWriter(fout,false));	
		bwriter.write("@RELATION ValidationResults"+"\r\n");
		int i,j,k;
		for(i=1;i<=validation_obj_num;i++){
			bwriter.write("@ATTRIBUTE "+i +" Real"+"\r\n" );
		}
	    bwriter.write("@DATA"+"\r\n");
	    for(j=0;j<file_list.length;j++){
		    for(k=0;k<validation_obj_num;k++){
				if(k==validation_obj_num-1){
				    if(validation_result[j][k]==1){
					    bwriter.write(1+"\r\n"); 
					}
				    else{
					    bwriter.write(0+"\r\n"); 
				    }
				}
				else{
				    if(validation_result[j][k]==1){
				    	bwriter.write(1+","); 
				    }
				    else{
				    	bwriter.write(0+","); 
				    }
			    }
			}
		}
		bwriter.close();     
	}
	
	//输出每个基分类器在验证集上的分类结果，“1”代表分类正确，“0”代表分类错误
    public void print_test_results() throws Exception{
		//构建存放文件的文件夹，用阈值命名
		File in=new File(this.filepath);
		String[] file_list=in.list();
		String path=in.getParent();
		path=path+"/classificationResults";
		File r=new File(path);
		if(!r.exists()){
			r.mkdirs();
		}
		//新文件命名
		System.out.println("写"+"TestResults"+"文件");
		String newname="/"+"TestResults.arff";
						
		//写文件流
		File fout = new File(path+newname);	
		BufferedWriter bwriter = new BufferedWriter(new FileWriter(fout,false));	
		bwriter.write("@RELATION TestResults"+"\r\n");
		int i,j,k;
		for(i=1;i<=validation_obj_num;i++){
			bwriter.write("@ATTRIBUTE "+i +" Real"+"\r\n" );
		}
		bwriter.write("@DATA"+"\r\n");
		for(j=0;j<file_list.length;j++){
			for(k=0;k<test_obj_num;k++){
				if(k==test_obj_num-1){
					if(test_result[j][k]==1){
						bwriter.write(1+"\r\n"); 
					}
					else{
						bwriter.write(0+"\r\n"); 
					}
				}
				else{
					if(test_result[j][k]==1){
						bwriter.write(1+","); 
					}
					else{
						bwriter.write(0+","); 
					}
				}
			}
		}
		bwriter.close();     
	}
    
    //依据输出结果的一致性计算相似性
	public double myDistance1(int[] first,int[] second){
		double n=0.0;
		for(int i=0;i<first.length;i++){
			if((first[i]==second[i])){
				n=n+1;
			}
		}
		return n/this.validation_obj_num-1;
	}
	
	
	//计算整体的相似性并输出相似性数据到文件，用于并行的紧邻传播聚类的输入文件
	public void writeSim()throws Exception{
		this.classifier();
		this.print_validation_results();
		this.print_test_results();
		sim=new double[classifier_num][classifier_num];//相似性矩阵
		for(int m=0;m<classifier_num;m++)
		{     
			for(int n=0;n<validation_obj_num;n++){
			    this.sim[m][n]=0.0;
				
			}
		}
		for(int i=0;i<classifier_num;i++){
			sim[i][i]=0;
			for(int j=i+1;j<classifier_num;j++){ 
				
				//依据距离公式计算相似性
				sim[i][j]=myDistance1(this.validation_result[i],this.validation_result[j]);
				sim[j][i]=sim[i][j];
				
				//不同类型重要度生成的基分类器相似性减0.5
				if((i%3)!=(j%3)){
				    sim[i][j]=sim[i][j]-0.5;
				    sim[j][i]=sim[j][i]-0.5;
				}
				
				//不同邻域阈值生成的基分类器相似性减邻域阈值的间隔
			    double interval=Math.abs(i-j)/100.0;
				sim[i][j]=sim[i][j]-interval;
				sim[j][i]=sim[j][i]-interval;
			}
		}
    	
		//将相似性输出到文件中
		File path=new File(this.filepath);
		String up_path=path.getParent()+"/input1/"+this.iter+"/";  
        File r=new File(up_path);
        if(!r.exists()){
    		r.mkdirs();
        }
    	String newfile=up_path+"Similarity.txt";
		File fout = new File(newfile);
		BufferedWriter bwriter= new BufferedWriter(new FileWriter(fout,true));
		for(int i=0;i<classifier_num;i++){
			bwriter.write(i+","+classifier_num+",");
			for(int j=0;j<classifier_num-1;j++){
				bwriter.write(sim[i][j]+",");
			}
			bwriter.write(sim[i][classifier_num-1]+"\r\n");
		}
		bwriter.close();
	}
	
	//对所有的基分类器进行AP聚类，得到类代表点和相应的聚簇信息
	public void runAP()throws Exception{
		
		//将相似性矩阵上传到HDFS
		this.writeSim();
		String target="hdfs://localhost:9000/hadoop/RoughSet/input1/"+this.iter+"/"+"Similarity.txt";
		FileInputStream fis=new FileInputStream(new File("/home/hadoop/workspace/Rough/data/input1/"+iter+"/Similarity.txt"));//读取本地文件
		Configuration config=new Configuration();
		FileSystem fs=FileSystem.get(URI.create(target), config);
		FSDataOutputStream os=fs.create(new Path(target));	  
		IOUtils.copyBytes(fis, os, 4096, true);
		System.out.println("拷贝完成...");
		
		//调用Parallel AP
		AP_Driver ap=new AP_Driver("hdfs://localhost:9000/hadoop/RoughSet/input1/"+iter+"/","hdfs://localhost:9000/hadoop/RoughSet/output4/"+iter+"/");
		ap.run();
		/*int chooseClassifier[]=new int[classifier_num];
		String outputPath="hdfs://localhost:9000/hadoop/RoughSet/output4/clusters/part-r-00000";
	    Configuration conf=new Configuration();
	    FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
	    FSDataInputStream in=fs.open(new Path(outputPath));
	    InputStreamReader ins=new InputStreamReader(in);
	    BufferedReader bReader=new BufferedReader(ins);
	    String line;
	    while((line=bReader.readLine())!=null){
		    String split[]=line.split("\t");
		    String split1[]=split[1].split(",");
		    int i=Integer.parseInt(split1[0]);
		    chooseClassifier[i]=1;
	    }
		return chooseClassifier;*/
	}
	
	//进行基分类器集成
	public double ensemble(){
		double ensemble_acc=0.0;
		try{
		    //this.runAP();
			this.writeSim();
			
		    //依据聚类结果进行基分类器的动态选择
		    String input="hdfs://localhost:9000/hadoop/RoughSet/output4/"+iter+"/";
		    System.out.println("基分类器的数量为"+classifier_num);
		  
		    Dynamic_selection d=new Dynamic_selection(validation_result,classifier_num,validation_obj_num,this.Nthreshold,input,this.sim);
		    int chooseClassifier[]=d.dynamicAdd();
		    K=K+d.getK();
		    
		    //计算被选择的基分类器个数和在各个测试样本被在这些被选择的基分类器上的分类结果
		    int correct_num[]=new int[test_obj_num];
		    int selectedCnum=0;
		    for(int i=0;i<this.classifier_num;i++){
		        if(chooseClassifier[i]==1){
		        	selectedCnum=selectedCnum+1;
		    	    for(int k=0;k<test_obj_num;k++){
		    		    if(test_result[i][k]==1){
		    			    correct_num[k]=correct_num[k]+1;
		    		    }
		    	    }
		        }
		    }
		    
		    //采用简单多数投票，超过一半的被选择的基分类器分类正确则对该样本分类正确
		    for(int i=0;i<test_obj_num;i++){
		    	if(correct_num[i]>(int)(selectedCnum*0.5)){
		    		ensemble_acc=ensemble_acc+1.0/test_obj_num;
		    		if(this.test_class[i]==1){
		    			tp=tp+1;
		    		}
		    		else{
		    			tn=tn+1;
		    		}
		    	}
		    	else{
		    		if(this.test_class[i]==1){
		    			fn=fn+1;
		    		}
		    		else{
		    			fp=fp+1;
		    		}
		    	}
		    }
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return ensemble_acc;
	}

	//打印集成分类结果
	public void printresult(){
		try{
			File path=new File(this.filepath);
			String up_path=path.getParent();
			String path1=up_path+"/Result/";
			File r=new File(path1);
		    if(!r.exists()){
		    	r.mkdirs();
		    } 
            String newfile=path1+"Acc.txt";
			File fout = new File(newfile);
			if(!fout.exists()){
				BufferedWriter bwriter= new BufferedWriter(new FileWriter(fout,true));
				bwriter.write("iter\tavg_em_acc\ttp\t\tfp\ttn\tfn\tK\r\n");
				bwriter.close();
			}
			BufferedWriter bwriter= new BufferedWriter(new FileWriter(fout,true));
			String line=this.num+"\t"+this.end_acc+"\t"+this.tp+"\t"+this.fp+"\t"+this.tn+"\t"+this.fn+"\t"+this.K+"\r\n";
			bwriter.write(line+"\r\n");
			int i;
			for(i=0;i<this.classifier_num-1;i++){
				bwriter.write(i+"\t"+this.acc_list[i]/this.num+"\t"+this.num_list[i]+"\r\n");//+"["+this.reduction_list.get(i)+"]"+";";
			}
			bwriter.write(i+"\t"+this.acc_list[i]/this.num+"\t"+this.num_list[i]);//["+this.reduction_list.get(this.classifier_num-1)+"]";
			bwriter.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void run(){
		try{
			this.read_info();
			System.out.println("读信息成功"); 
			
			for(iter=0;iter<this.num;iter++){
				this.train_obj_num=0;
				this.validation_obj_num=0;
				double temp_acc=this.ensemble();
				System.out.println("第"+(iter+1)+"次分类成功");
				System.out.println("temp_acc:"+temp_acc);
				this.end_acc=this.end_acc+temp_acc;
			}
			this.end_acc=this.end_acc/this.num;
			this.fn=fn/this.num;
			this.fp=fp/this.num;
			this.tn=tn/this.num;
			this.tp=tp/this.num;
			this.K=this.K/this.num;
			System.out.println("分类成功");
			
			this.printresult();
			System.out.println("打印结果成功");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
