import java.io.File;
import java.math.BigDecimal;
public class Main_procedure {
	
	public static void main(String args[]){
		try{
			String data_file="/home/hadoop/workspace/Rough/data/ArabidopsisTEV300.arff";
		    for(float threshold=0.01f;threshold<=0.65f;threshold=threshold+0.01f){
		    	
		    	BigDecimal b =new BigDecimal(threshold);
			    threshold=b.setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
			    
			    //第一阶段生成input的数据
			    Read_info relation=new Read_info(data_file,threshold);
			    //relation.write_attr();
			    
			    //第二阶段产生各个属性下样本之间的相交关系
			    //relation.run();
			    
			    //第三阶段产生全属性下的相交邻域
			    /*int [] reduction_attr_list=new int[relation.getAttr_num()];
			    for(int i=0;i<relation.getAttr_num();i++){
			    	reduction_attr_list[i]=1;
			    }
			    //计算全属性下的正域
			    int p_intersection_neighborhood[][]=new int [relation.getObject_num()][relation.getObject_num()];
			    Intersection_neighborhood neighborhood=new Intersection_neighborhood(reduction_attr_list,relation,"hdfs://localhost:9000/hadoop/RoughSet/output1/"+threshold+"/ar_all/");
			    neighborhood.run();//计算全属性下每个属性的相交邻域*/
			    
			    //第四阶段进行属性约简
			    /*Back_reduction ar=new Back_reduction(data_file,threshold,relation,"positive_region");
			    ar.run();
			    ar.setRank("Information");
			    ar.run();
			    ar.setRank("WRank");
			    ar.run();*/
			}
		    
		    //第五阶段产生基分类器，构造相似性矩阵，对基分类器进行parallel Ap聚类，依据聚类结果进行基分类器动态选择，并输出集成结果
		    File in=new File(data_file);
		    String path=in.getParent();
		    path=path+"/Red_data/";
	        double per=0.4;//训练集所占比例
	        int n=10;//迭代次数
	        int N_threshold=50;//集成模型最大基分类器数量
            Classify c=new Classify(path,per,n,N_threshold);
	    	c.run();
		}
        catch(Exception e){
        	e.printStackTrace();
		}
	}
}
