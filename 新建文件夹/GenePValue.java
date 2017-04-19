import java.util.ArrayList;
import org.apache.commons.math3.stat.inference.MannWhitneyUTest;

/***
 * ������Ϣ�ĳ�����
 * @author �ž�������
 *
 */
public class GenePValue {
	private int gene_num;//��¼�����ڴ����ݼ����ǵڼ�������
	private String gene_id;//��¼����ID���൱�ڻ�������
	private double p_value;//��¼�˻����pֵ
	private int rank;//��¼���������λ��
	private ArrayList<Double> class1;//��¼��һ������
	private ArrayList<Double> class2;//��¼�ڶ�������
	//���캯��
	public GenePValue(){
		rank=-1;
		p_value=-1;
		class1=new ArrayList<Double>();
		class2=new ArrayList<Double>();
	}
	public GenePValue(int num,String id){
		gene_num=num;
		gene_id=id;
		rank=-1;
		p_value=-1;
		class1=new ArrayList<Double>();
		class2=new ArrayList<Double>();
	}
	
	
	public int getgenenum(){
		return gene_num;
	}
	public void setgenenum(int num){
		gene_num=num;
	}
	
	public String getgeneid(){
		return gene_id;
	}
	public void setgeneid(String id){
		gene_id=id;
	}
	
	public double getpvalue(){
		return p_value;
	}
	public void computepvalue(){
		double[] a=new double[class1.size()];
		double[] b=new double[class2.size()];
		for(int i=0;i<a.length;i++){
			a[i]=class1.get(i).doubleValue();
		}
		for(int i=0;i<b.length;i++){
			b[i]=class2.get(i).doubleValue();
		}
		MannWhitneyUTest u=new MannWhitneyUTest();
		p_value=u.mannWhitneyUTest(a, b);
	}
	
	public int getrank(){
		return rank;
	}
	public void setrank(int r){
		rank=r;
	}
	
	public ArrayList<Double> getclass(int flag){
		if(flag==1){
			return class1;
		}
		else{
			return class2;
		}
	}
	public void add(int flag,String data){
		Double value;
		value=Double.valueOf(data);
		if(flag==1){
			class1.add(value);
		}
		else{
			class2.add(value);
		}
	}
	
}
