import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;


public class Dynamic_selection {
    private int classifier_num;//基分类器总数
    private int selected_classifier_num=0;//被选择的基分类器数量
    private int validation_num=0;//验证集样本数量
    private int exemplars_num=0;//代表点数量
    private int validation[][];//验证集上所有基分类器的分类结果
    private int[][] clusters;;//聚簇信息，如果第j个基分类器在以第i个基分类器为代表点的聚簇中则clusters[i][j]=1
    private int [] exemplars;//代表点对应的基分类器，是代表点则对应为 1
    private  int Nthreshold;//选择的基分类器数量阈值
    private String input;//hdfs路径
    private double similarity[][];//基分类器之间的相似性矩阵
    private double K1;
    public Dynamic_selection(int validation[][],int classifiers_num,int validations_num,int Nthreshold,String input,double sim[][]){
    	this.validation=validation;
    	this.classifier_num=classifiers_num;
    	this.validation_num=validations_num;
    	this.Nthreshold=Nthreshold;
    	this.input=input;
    	this.similarity=sim;
    	
    	
    }
    
    //计算被选择的基分类器的平均分类准确率
    public double averageAccuracy(int selectedClassifier[]){
    	double p=0.0;
    	int num=0;
    	for(int i=0;i<classifier_num;i++){
    		if(selectedClassifier[i]==1){
    			num=num+1;
    		    for(int j=0;j<validation_num;j++){
    		    	if(validation[i][j]==1){
    		            p=p+1;
    		    	}
    			}
    		}
    	}
    	p=p/(num*(validation_num));
    	return p;
    }
    
    //计算总体Dis 
    public double getDis(int selectedClassifier[]){
    	double dis=0.0;
    	for(int i=0;i<classifier_num;i++){
    		for(int j=0;j<classifier_num;j++){
    			if(selectedClassifier[i]==1&&selectedClassifier[j]==1){
    			    if(j==i){
    				    continue;
    			    }
    			    dis=dis+getDisij(validation[i],validation[j]);
    			}
    		}
    	}
    	dis=dis/(selected_classifier_num*(selected_classifier_num-1));
    	return dis;
    }
    
    //计算两个基分类器之间的Dis
    public double getDisij(int hi[],int hj[]){
    	double dis=0.0;
    	int num=hi.length;
    	int n00=0;
    	int n01=0;
    	int n10=0;
    	int n11=0;
    	for(int i=0;i<num;i++){
    		if(hi[i]==0){
    			if(hj[i]==0){
    				n00=n00+1;
    			}
    			else{
    				n01=n01+1;
    			}
    		}
    		else{
    			if(hj[i]==0){
    				n10=n10+1;
    			}
    			else{
    				n11=n11+1;
    			} 
    		}
    	}
    	dis=((double)(n10+n01))/num;
    	return dis;
    }
    
    //计算interrater agreement K
    public double getK(int selectedClassifier[]){
    	double p=averageAccuracy(selectedClassifier);
    	System.out.println("p="+p);
    	double dis=getDis(selectedClassifier);
    	System.out.println("dis="+dis);
    	double k=1-(1/(2*p*(1-p)))*dis;
    	System.out.println("k="+k);
    	return k;
    }
    
    //对每个聚簇中的基分类器依据分类准确性排序
    public int[] sort(int[] cluster,int [][]validation){
    	int classifiers_num_cluster=0;
    	int accuracy[]=new int[classifier_num];
    	
    	//统计聚簇中每个基分类器在验证集上的分类准确率
    	for(int i=0;i<classifier_num;i++){
    		if(cluster[i]==1){
    			classifiers_num_cluster=classifiers_num_cluster+1;
    			for(int j=0;j<validation_num;j++){
    				if(validation[i][j]==1){
    			        accuracy[i]=accuracy[i]+1;
    				}
    			}
    			accuracy[i]=accuracy[i]/validation_num;
    		}
    	}
    	
    	//对排序数组进行初始化
    	int rank_classifier[]=new int[classifier_num];
    	int m=0;
    	for(int i=0;i<classifier_num;i++){
    		if(cluster[i]==1){
    			rank_classifier[m]=i;
    			m++;		
    		}	
    	}
    	
    	//对基分类器的标号进行排序
    	for(int i=0;i<classifiers_num_cluster;i++){
    		int min=i;
    		for(int j=i+1;j<classifiers_num_cluster;j++){
    			if(accuracy[rank_classifier[min]]>accuracy[rank_classifier[j]]){
    				min=j;
    			}
    		}
    		if(min!=i){
        		int temp=rank_classifier[i];
        		rank_classifier[i]=rank_classifier[min];
        		rank_classifier[min]=temp;
    	    }
        }
    	
    	//对于不在聚簇中的基分类器赋值-1
    	for(int i=classifiers_num_cluster;i<classifier_num;i++){
    		rank_classifier[i]=-1;
    	}
    	
    	return rank_classifier;
    }
    
    //从hdfs中得到聚簇
    /*public int[][] getSortedClusters()throws Exception{	   
        int accuracy_sort[][]=new int[classifier_num][classifier_num];
        clusters=new int[classifier_num][classifier_num];
        exemplars=new int[classifier_num];
        String outputPath=this.input+"final/part-r-00000";
	    Configuration conf=new Configuration();
	    FileSystem fs=FileSystem.get(URI.create(outputPath),conf);
	    FSDataInputStream in=fs.open(new Path(outputPath));
	    InputStreamReader ins=new InputStreamReader(in);
	    BufferedReader bReader=new BufferedReader(ins);
	    String line=null;
	    
	    //从文件中读取聚簇信息，得到聚簇的矩阵，
	    while((line=bReader.readLine())!=null){
	    	int i;
	    	int j;
	    	String split[]=line.split("\t");
	    	i=Integer.parseInt(split[1]);
	    	clusters[i][i]=1;//把类代表点本身也加入到聚簇中	
	    	exemplars[i]=1;
	    	j=Integer.parseInt(split[0]);
	    	clusters[i][j]=1;//表示基分类器j在以基分类器i为代表点的聚簇中
	    	
        } 
	    
	    //对每个聚类中的基分类器依据分类准确性进行排序
        for(int i=0;i<classifier_num;i++){
        	if(exemplars[i]==1){
        	    accuracy_sort[i]=sort(clusters[i],validation);
        	}
        }
        
        //统计类代表点的数量，即聚类的簇数
        for(int i=0;i<classifier_num;i++){
        	if(exemplars[i]==1){
        		this.exemplars_num=this.exemplars_num+1;
        	}
        }
        
        return accuracy_sort;
    }*/
    
    //计算当前被选择的基分类器采用简单多数投票法在验证集上的分类准确率
    public double getEnsembleAccuracy(int []selected_classifiers){
    	double ensemble_accuracy=0.0;
    	int selected_classifier_num=0;
    	
    	//计算当前基分类器数量
    	for(int i=0;i<classifier_num;i++){
    		if(selected_classifiers[i]==1){
    			selected_classifier_num=selected_classifier_num+1;
    		}
    	}
    	
    	//统计每个验证样本在被选择的基分类器上的分类结果
    	int []correct_num=new int[validation_num];	
        for(int i=0;i<classifier_num;i++){
        	if(selected_classifiers[i]==1){
		        for(int j=0;j<validation_num;j++){
		    	    if(validation[i][j]==1){
		    		    correct_num[j]=correct_num[j]+1;	
		    	    }		
		        }
        	}
		}
        
        //进行多数投票
        for(int i=0;i<validation_num;i++){
	    	if(correct_num[i]>(int)(selected_classifier_num*0.5)){
	    		ensemble_accuracy=ensemble_accuracy+1.0/validation_num;
	    	}
        }
        
    	return ensemble_accuracy;
    }
    
    //依据相似性矩阵和聚类中心进行基分类器的划分，并得到排序过的基分类器聚簇
    public int[][] getSortedClusters()throws Exception{
    	int clusters[][]=new int[classifier_num][classifier_num];
    	int accuracy_sort[][]=new int[classifier_num][classifier_num];
	    AP1 ap=new AP1(similarity,classifier_num);
        exemplars=ap.getExemplar();
        //对每个聚类中的基分类器依据分类准确性进行排序
        for(int i=0;i<classifier_num;i++){
        	int minnum=0;
        	double min=1000;
        	for(int j=0;j<classifier_num;j++){
        		if(exemplars[j]==1){
        			if(similarity[i][j]<min){
        				minnum=j;
        				min=similarity[i][j];
        			}
        		}
        	}
        	clusters[minnum][i]=1;
        }
        for(int i=0;i<classifier_num;i++){
        	if(exemplars[i]==1){
        	    accuracy_sort[i]=sort(clusters[i],validation);
        	}
        }
        
        //统计类代表点的数量，即聚类的簇数
        for(int i=0;i<classifier_num;i++){
        	if(exemplars[i]==1){
        		this.exemplars_num=this.exemplars_num+1;
        	}
        }
	    return accuracy_sort;
	}
    
    //进行动态集成选择
    public int [] dynamicAdd()throws Exception{
        int selected_classifiers[]=new int[classifier_num];
        int sortedClusters[][]=getSortedClusters();
        
        //初始化基分类器选择数组
        for(int i=0;i<classifier_num;i++){
        	selected_classifiers[i]=0;
        }
        
        //将每个聚类中基分类器的排序结果输出
        /*for(int i=0;i<classifier_num;i++){
        	if(exemplars[i]==1){
        		System.out.print(i+",");
        	    int j;
        	    for(j=0;j<classifier_num-1;j++){
        		    System.out.print(sortedClusters[i][j]+" ");
        	    }
        	    System.out.println(sortedClusters[i][j]);
        	}
        }*/
        
        double ensemble_accuracy=0.0;//添加基分类器之前的准确率
        double accuracy=1.0;//开始的准确率要求
        double lamude=0.01;//准去率递减的间隔
        double ensemble_accuracy1=0.0;//当前集成的准确率

        //进行动态集成，其中accuracy要求动态变化
        while(accuracy>0){		
        	System.out.println("当前准确率为"+accuracy);
        	
        	//将集成选择的结果重置为只有类代表点
        	for(int i=0;i<classifier_num;i++){
                if(exemplars[i]==1){
             	    selected_classifiers[i]=1;
                }
            }
        	
        	selected_classifier_num=0;
        	for(int n=0;n<classifier_num;n++){
                if(selected_classifiers[n]==1){
	                selected_classifier_num++;
                }
            }
        	
        	ensemble_accuracy1=getEnsembleAccuracy(selected_classifiers);
    	    if(ensemble_accuracy1>=accuracy){
                return selected_classifiers;
            }
            
    	    int interaction=0;//最大迭代次数
        	//当集成的准确率和集成的分类器数量没达到阈值时，不断添加基分类器
            while((ensemble_accuracy1<accuracy)&&(selected_classifier_num<Nthreshold)&&(interaction<1000)){
                for(int k=1;k<classifier_num;k++){
        	        int i;
                    for(i=0;i<this.classifier_num;i++){
                    	if(exemplars[i]==1){
            	            if(sortedClusters[i][k]!=-1){
            	            	
            	            	//计算初始的一致性和集成准确率
            	        	    double K=this.getK(selected_classifiers);
            	        	    System.out.print("初始不一致性为："+K+" ");
            	        	    ensemble_accuracy=getEnsembleAccuracy(selected_classifiers);
            	        	    System.out.println("初始集成的准确率为："+ensemble_accuracy);
            	        	    
            	        	    //每个聚类中按照分类准确性顺序加入基分类器，每次加入一个
            	        	    if(selected_classifiers[sortedClusters[i][k]]==0){
            	        		    System.out.println("添加类代表点"+i+"的第"+k+"个基分类器");
            		                selected_classifiers[sortedClusters[i][k]]=1;
            		                interaction=interaction+1;
            		                
            		                //计算当前集成的基分类器数量
            		                selected_classifier_num=0;
            		                for(int n=0;n<classifier_num;n++){
            			                if(selected_classifiers[n]==1){
            				                selected_classifier_num++;
            			                }
            		                }
            		                System.out.println("前集成的基分类器数量"+selected_classifier_num);
            		                
            		                //计算当前的一致性和集成分类准确率
            		                K1=getK(selected_classifiers);
            		                System.out.print("不一致性为："+K1+" ");
            		                ensemble_accuracy1=getEnsembleAccuracy(selected_classifiers);
            		                System.out.println("集成的准确率为："+ensemble_accuracy1);
            		                
            		                //如果K值减小并且集成准确率提高则确定加入当前选择的基分类器，否则回滚
            		                if((K1<=K)&&(ensemble_accuracy1>=ensemble_accuracy)){
            		                	
            		                	//如果当前集成的分类准确率大于当前的准确率要求或者分类器数量超出阈值则停止添加基分类器
            		                    if(ensemble_accuracy1>=accuracy){
            			                    break;
            		                    }
            		                    if(selected_classifier_num>=Nthreshold){
            		                     	break;
            		                    }
            		                }
            		                else{
            		                	ensemble_accuracy1=ensemble_accuracy;
            		                	selected_classifier_num=selected_classifier_num-1;
            			                selected_classifiers[sortedClusters[i][k]]=0;
            			                System.out.println("去掉该点");
            		                }
            	                }
                            }
                        }
                    }	
                    if(ensemble_accuracy1>=accuracy||selected_classifier_num>=Nthreshold){
            	        break;
                    }
                }  
            }
            if(ensemble_accuracy1>=accuracy){//满足准去率要求停止动态集成
            	accuracy=-1.0;
            }
            else{
                accuracy=accuracy-lamude;//自动降低准确率要求
            }
        }
        
        //计算被选择的基分类器的数量为
        int n=0;
        for(int i=0;i<classifier_num;i++){
        	if(selected_classifiers[i]==1){
        		n=n+1;
        	}
        }
        System.out.println("被选择的基分类器的数量为"+n);
        
        return selected_classifiers;
    }
    public double getK(){
    	return K1;
    }
}
