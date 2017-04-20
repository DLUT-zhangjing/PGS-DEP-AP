public class Positive_region {
	private int object_num;//样本个数
	private int intersection_neighborhood[][];//相交邻域
	private int equ_class[][];//等价类列表
	private int positive_region[][];//每个等价类相应相交邻域下的正域
	private int positive_region_all[];//相应相交邻域下的正域
	private Read_info relations;//数据集信息，包含每个属性下样本之间的相交关系，样本属性，类别等信息
	
	public Positive_region(Read_info relation,int [][] p_intersection_neighborhood){
		this.relations=relation;
		this.object_num=relations.getObject_num();
		this.intersection_neighborhood=new int [object_num][object_num];
		this.intersection_neighborhood=p_intersection_neighborhood;
		this.equ_class=new int[2][object_num];
		equ_class=relations.getEqu_class();
	}
	
	public void setIntersection_neighborhood(int [][] p_intersection_neighborhood){
		this.intersection_neighborhood=new int[object_num][object_num];
		this.intersection_neighborhood=p_intersection_neighborhood;
	}
	
	public int[] getPositive_region_all()throws Exception{
		this.positive_region();
		return this.positive_region_all;
	}
	
	//进行正域计算
	public int[] positive_region()throws Exception{
		positive_region=new int[2][object_num];
		positive_region_all=new int[object_num];
		int i;
		for(i=0;i<2;i++){
			for(int j=0;j<object_num;j++){
				positive_region[i][j]=1;
			}
		}
		for(int n=0;n<2;n++){
		    for(i=0;i<object_num;i++){  
			    for(int j=0;j<object_num;j++){
				    if(intersection_neighborhood[i][j]==0)
				    	positive_region[n][i]=1;
				    else{
				    	if(equ_class[n][j]==1){
				            positive_region[n][i]=1;
				        }
				    	else{
				    		positive_region[n][i]=0;
				    		break;
				    	}
				    }
			    }
		    }
		}
		
		//将两个等价类对应的正域进行合并
		for(i=0;i<object_num;i++){
		    if((positive_region[0][i]==1)||(positive_region[1][i]==1))
			    positive_region_all[i]=1;
			else
			    positive_region_all[i]=0;
		    }
		return positive_region_all;
	}
	public int getNum(){
		int n=0;
		for(int i=0;i<object_num;i++){
			if(positive_region_all[i]==1){
				n=n+1;
			}
		}
		return n;
	}
}
