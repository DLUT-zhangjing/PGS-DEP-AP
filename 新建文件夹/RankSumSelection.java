import java.io.*;
import java.util.ArrayList;

/***
 * ����Ⱥͼ�ⷽ�����ǰ...������
 * @author �ž�������
 *
 */
public class RankSumSelection {
	private String filepath;//����ļ�·��
	private int top_num;//ѡ��������
	private ArrayList<GenePValue> top_gene;//ѡ���Ļ�����б�
	private ArrayList<GenePValue> gene_list;//ȫ��������Ϣ�б�
	
	//���캯��
	public RankSumSelection(int top,String datapath){
		filepath=datapath;
		top_num=top;
		top_gene=new ArrayList<GenePValue>();
		gene_list=new ArrayList<GenePValue>();
	}
	//������ļ�����ȡȫ��������Ϣ
	public void readfile() throws Exception{
		System.out.println("read");
		FileInputStream in=new FileInputStream(filepath);
		InputStreamReader inReader=new InputStreamReader(in);
		BufferedReader bReader=new BufferedReader(inReader);
		String line=null;
		int i=0;
		while((line=bReader.readLine())!=null){
			if(line!=""){
				String split1[]=line.split(" ");
				if(split1.length>=3){
					if(split1[0].toUpperCase().equals("@ATTRIBUTE") && split1[2].charAt(0)!='{'){
						GenePValue newgene = new GenePValue(i,split1[1]);
						gene_list.add(newgene);
//						System.out.println(i);
						i++;
					}
				}
				if(line.substring(0,5).equals("@DATA")){
//					System.out.println("a");
//					line=bReader.readLine();
					break;
				}	
			}
//			System.out.println(line);
		}
		String class1_name=null;
		while((line=bReader.readLine())!=null){
			if(line!=""){
				String split2[]=line.split(",");
				if(class1_name==null){//ȡ��һ�γ��ֵ��������Ϊ��һ����
					class1_name=split2[split2.length-1];
				}
				if(split2[split2.length-1].equals(class1_name)){
					for(int j=0;j<gene_list.size();j++){
						GenePValue tempgene=gene_list.get(j);
						tempgene.add(1, split2[j]);
					}
				}
				else{
					for(int j=0;j<gene_list.size();j++){
						GenePValue tempgene=gene_list.get(j);
						tempgene.add(2, split2[j]);
					}
				}
			}
		}
		bReader.close();
		inReader.close();
		in.close();
		//����ÿ�������pֵ
		for(int j=0;j<gene_list.size();j++){
			GenePValue tempgene=gene_list.get(j);
			tempgene.computepvalue();
//		System.out.println(tempgene.getpvalue());
		}
	}
	//��ݻ����pֵ�����ǰtop_num������
	public ArrayList<GenePValue> gettop(){
		System.out.println("gettop");
		int i=1;
		while(i<=top_num){
			//System.out.println(i);
			double minpvalue=1;
			int min_index=-1;
			for(int j=0;j<gene_list.size();j++){//����δ����Ļ�����pֵ��С��һ������
				GenePValue tempgene=gene_list.get(j);
				if(tempgene.getrank()==-1 && minpvalue>tempgene.getpvalue()){
					minpvalue=tempgene.getpvalue();
					min_index=j;
				}
			}
			if(min_index!=-1){//�ҵ����ʵĻ���������rankֵ,�����ӵ�top_gene����
				for(int j=0;j<gene_list.size();j++){
					GenePValue tempgene=gene_list.get(j);
					if(tempgene.getgenenum()==min_index){
						tempgene.setrank(i);
						top_gene.add(tempgene);
					}
				}
			}
			i++;
		}
		return top_gene;
	}
	
	/*//���ѡ���Ļ���ɾ������ԭʼ���
	public void selectdata()throws Exception{
		//���ļ���
		FileInputStream in=new FileInputStream(filepath);
		InputStreamReader inReader=new InputStreamReader(in);
		BufferedReader bReader=new BufferedReader(inReader);
		String rline=null;
		
		//д�ļ���
		File fout = new File(this.output);	
		BufferedWriter bwriter = new BufferedWriter(new FileWriter(fout,false));	
		//���ж�ԭ�ļ�������������ļ�
		ArrayList<String> attr=new ArrayList<String>();
		//System.out.println("a");
		int flag=0;
		while((rline=bReader.readLine())!=null){
			//System.out.println("b");
			//System.out.println(rline);
			if(rline.substring(0,5).toUpperCase().equals("@DATA")){
			//System.out.println("d");
				flag=1;
				break;
			}
			else if(rline.substring(0,10).toUpperCase().equals("@ATTRIBUTE")){
			//System.out.println("c");
				attr.add(rline);
			}
			else{
			//System.out.println(rline);
				bwriter.write(rline+"\r\n");
			}
		}
		if(flag==1){//������ݱ�־���˳�ʱ�����
			for(int j=0;j<top_gene.size();j++){//���ѡ���Ļ�������
				GenePValue tempgene=top_gene.get(j);
				int index=tempgene.getgenenum();
				bwriter.write(attr.get(index)+"\r\n");
			}
			bwriter.write(attr.get(attr.size()-1)+"\r\n");//����������
			bwriter.write(rline+"\r\n");//���@DATA
			while((rline=bReader.readLine())!=null){//������ݲ���
				String outdata=null;
				String split[]=rline.split(",");
				for(int j=0;j<top_gene.size();j++){//���ѡ���Ļ������
					GenePValue tempgene=top_gene.get(j);
					int index=tempgene.getgenenum();
					if(outdata==null){
						outdata=split[index];
					}
					else{
						outdata+=","+split[index];
					}
				}
				outdata+=","+split[split.length-1];
				bwriter.write(outdata+"\r\n");
			}
		}
		bwriter.close();
		bReader.close();
		inReader.close();
		in.close();
		File r=new File(filepath);
		if(filepath.equals("E:\\20114476\\��ҵ����(�ž�)\\����ʵ��\\���\\data\\00.arff")){
		    r.delete();
		}

	}
	*/
	//���pֵͳ�ƻ������ֲ�
	public void p_gene_num(){
		int[] genenum=new int[21];//0��¼0-0.01�Ļ������1-20�ֱ��¼0-1step0.05�Ļ������
		for(int i=0;i<21;i++){
			genenum[i]=0;
		}
		for(int j=0;j<gene_list.size();j++){
			double p=gene_list.get(j).getpvalue();
			if(p<=0.01){
				genenum[0]++;
			}
			if(p<=0.05){
				genenum[1]++;
			}
			else if(p<=0.1){
				genenum[2]++;
			}
			else if(p<=0.15){
				genenum[3]++;
			}
			else if(p<=0.20){
				genenum[4]++;
			}
			else if(p<=0.25){
				genenum[5]++;
			}
			else if(p<=0.30){
				genenum[6]++;
			}
			else if(p<=0.35){
				genenum[7]++;
			}
			else if(p<=0.40){
				genenum[8]++;
			}
			else if(p<=0.45){
				genenum[9]++;
			}
			else if(p<=0.50){
				genenum[10]++;
			}
			else if(p<=0.55){
				genenum[11]++;
			}
			else if(p<=0.60){
				genenum[12]++;
			}
			else if(p<=0.65){
				genenum[13]++;
			}
			else if(p<=0.70){
				genenum[14]++;
			}
			else if(p<=0.75){
				genenum[15]++;
			}
			else if(p<=0.80){
				genenum[16]++;
			}
			else if(p<=0.85){
				genenum[17]++;
			}
			else if(p<=0.90){
				genenum[18]++;
			}
			else if(p<=0.95){
				genenum[19]++;
			}
			else if(p<=1){
				genenum[20]++;
			}
		}
		for(int k=0;k<21;k++){
			System.out.println(k+": num"+genenum[k]);
		}
	}
	
	public void run()throws Exception{
		readfile();
		System.out.println("���ļ��ɹ�");
		p_gene_num();
		/*selectdata();
		System.out.println("д�ļ��ɹ�");*/
	}
}
