import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AP {

	private int top_k;
	private int iter = 1000;
	private double similar[][];
	private double r[][];
	private double a[][];
	private double lambda = 0.9;
	
	public ArrayList<Point> exemplar = new ArrayList<Point>();
	int idx[]; // the id of exemplar for each point
	
	/*//融合两种相似矩阵，参数threshold
		void incorporate(double threshold, double expsim[][], double gosim[][]) {
			for(int i=0; i<top_k; i++){
				for(int j=top_k -1; j>i; j--){
					similar[i][j] = (double) -((1-threshold)*expsim[i][j] + threshold*(1 - gosim[i][j]));
					similar[j][i] = similar[i][j];
				}
			}
			
			for(int i=0; i<top_k; i++){
				for(int j=0; j<top_k; j++){
					System.out.print(similar[i][j]+" ");
				}
				System.out.println();
			}
		}
	*/
	// 通过数据点信息构造相似度矩阵
	void constructMatrix(List<Point> points) {
		for(int i=0; i<top_k; i++){
			for(int j=i+1; j<top_k; j++){
				int t = points.get(j).id;
				similar[i][j] = points.get(i).value.get(t);
				similar[j][i] = similar[i][j];
			}
		}
	}
		
	//calculate the preference
	void preference(){
		//use the median as preference
		int m = 0;
		int n = top_k * (top_k-1) / 2;
		double median = 0;
		double list[] = new double[n];
		
		//find the median
		for(int i=0; i<top_k; i++){
			for(int j=i+1; j<top_k; j++){
				list[m++]=similar[i][j];
			}
		}
		Arrays.sort(list);
		if(n%2 == 0){
			median = (list[n/2] + list[n/2-1]) / 2;
		}
		else{
			median = list[n/2];
		}
		for(int i=0; i<top_k; i++){
			similar[i][i] = median;
		}
		
		//test output
		/*for(int i=0; i<n; i++){
			System.out.println(i+" "+list[i]);
		}*/
		System.out.println(median);
		/*for(i=0; i<top_k; i++){
			for(j=0; j<top_k; j++){
				System.out.print(similar[i][j] + " ");
			}
			System.out.println();
		}*/
	}
	
	void responsi(){
		for(int i=0; i<top_k; i++){
			for(int k=0; k<top_k; k++){
				double max = -65535;
				for(int j=0; j<top_k; j++){
					if(j != k){
						if(max < a[i][j]+similar[i][j]){
							max = a[i][j] + similar[i][j];
						}
					}
				}
				r[i][k] = (1-lambda)*(similar[i][k] - max) + lambda*r[i][k];
			}
		}
	}
	
	void availa(){
		for(int i=0; i<top_k; i++){
			for(int k=0; k<top_k; k++){
				if(i == k){
					double sum = 0;
					for(int j=0; j<top_k; j++){
						if(j != k){
							if(r[j][k] > 0){
								sum += r[j][k];
							}
						}
					}
					a[k][k] = sum;
				}
				else{
					double sum = 0;
					for(int j=0; j<top_k; j++){
						if(j!=i && j!=k){
							if(r[j][k] > 0){
								sum += r[j][k];
							}
						}
					}
					a[i][k] = (1-lambda)*(r[k][k] + sum) + lambda*a[i][k];
					if(a[i][k] > 0){
						a[i][k] = 0;
					}
				}
			}
		}
	}
	
	void clustering(List<Point> points){
		//iteratively calculate responsibility and availability
		for(int i=0; i<iter; i++){
			responsi();
			availa();
			System.out.println("iter " + i +" complete");
		}
		
		//find the clustering centers
		for(int k=0; k<top_k; k++){
			if(r[k][k]+a[k][k] > 0){
				exemplar.add(points.get(k));
			}
		}
		System.out.println("exemplar top_k:" + exemplar.size());
		for(int k=0; k<exemplar.size(); k++){
			System.out.println("exemplar:" + exemplar.get(k).id);
		}
		
		//update the diagonal similarity for the next step
		/*for(int k=0; k<top_k; k++){
			similar[k][k] = 1;
		}*/
		
		//data point assignment
		/*for(int i=0; i<top_k; i++){
			double max = -65535;
			int index = 0;
			for(int j=0; j<exemplar.size(); j++){
				int k = exemplar.get(j);
				if(max < similar[i][k]){
					max = similar[i][k];
					index = k;
				}
			}
			idx[i] = index;
		}*/
		
		//result output
		/*for(int i=0; i<top_k; i++){
			System.out.println(i+2 + ": " +idx[i]);
		}*/
		/*int n = 0;
		File testfile = new File("D:\\test.csv");
		BufferedWriter bw;
		try {
			bw = new BufferedWriter(new FileWriter(testfile, true));
			for(int m=0; m<top_k; m++){
				for(n=0; n<top_k-1; n++){
					bw.write(similar[m][n] + ",");
				}
				bw.write(similar[m][n] + "\r\n");
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
	}
	
	//output data file with cluster center
	/*void outputfile(String top_filepath, String cent_filepath, double threshold) throws IOException {
		int flag = 0;
		int i = 0;
		int k = 0;
		File topfile = new File(top_filepath);
		File centfile = new File(cent_filepath);
		
		FileInputStream fis = new FileInputStream(topfile);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		BufferedWriter bw = new BufferedWriter(new FileWriter(centfile, true));
		String line = null;
		
		while((line=br.readLine()) != null){
			String split[] = line.split("\\s+");
			if(split[0].equals("@RELATION")){
				bw.write(line + "\r\n");
			}
			if(split[0].equals("@ATTRIBUTE")){
				if(split[1].equals("Class")){
					bw.write(line + "\r\n");
				}
				if(k >= exemplar.size()){
					continue;
				}
				if(exemplar.get(k) == i){
					bw.write(line + "\r\n");
					k++;
				}
				i++;
			}
			if(split[0].equals("@DATA")){
				flag = 1;
				bw.write(line + "\r\n");
				continue;
			}
			if(flag == 1){
				String data[] = line.split(",");
				for(int j=0; j<exemplar.size(); j++){
					bw.write(data[exemplar.get(j)] + ",");
				}
				bw.write(data[data.length-1]);
				bw.write("\r\n");
			}
			
		}
		bw.close();
		br.close();
		isr.close();
		fis.close();
	}*/
	
	//output file with gene subsets from clustering
	/*void subsetfile(String topfilepath, double threshold, String subset_filepath) throws IOException {
		
		int e = 0;
		File topfile = new File(topfilepath);
		
		for(int i=0; i<exemplar.size(); i++){
			e = exemplar.get(i);//记录当前样本点
			FileInputStream fis = new FileInputStream(topfile);
			InputStreamReader isr = new InputStreamReader(fis);
			BufferedReader br = new BufferedReader(isr);
			
//			String subsetfilepath = "D:\\workspace\\GOAPNR\\Data Set\\select\\ArabidopsisDrought\\subset\\all_" + threshold + "\\"
//					+ threshold + "_subset" + i + "_" + topfile.getName();
			String subpath = subset_filepath + threshold + "_subset" + i + "_" + topfile.getName();
			File subsetfile = new File(subpath);
			BufferedWriter bw = new BufferedWriter(new FileWriter(subsetfile, true));
			
			String line = null;
			int count = 0;//记录基因序数
			int flag = 0;
			while((line=br.readLine()) != null){
				String split[] = line.split("\\s+");
				if(split[0].equals("@RELATION")){
					bw.write(line + "\r\n");
				}
				if(split[0].equals("@ATTRIBUTE")){
					if(split[1].equals("Class")){
						bw.write(line + "\r\n");
					}
					else if(e == idx[count]){
						bw.write(line + "\r\n");
					}
					count++;
				}
				if(split[0].equals("@DATA")){
					flag = 1;
					bw.write(line + "\r\n");
					continue;
				}
				if(flag == 1){
					String data[] = line.split(",");
					for(int j=0; j<data.length-1; j++){
						if(idx[j] == e){
							bw.write(data[j] + ",");
						}
					}
					bw.write(data[data.length-1]);
					bw.write("\r\n");
				}
			}
			bw.close();
			br.close();
			isr.close();
			fis.close();
		}
		
		//output exemplars file
		String exemfilepath = subset_filepath + "exemplars.txt";
		File exemfile = new File(exemfilepath);
		BufferedWriter bw0 = new BufferedWriter(new FileWriter(exemfile, true));
		for(int i=0; i<exemplar.size(); i++){
			bw0.write(exemplar.get(i) + "\r\n");
		}
		bw0.write("#####\r\n");
		for(int j=0; j<idx.length; j++){
			bw0.write(idx[j] + "\r\n");
		}
		bw0.close();
		
	}*/

	/*public AP(String filepath1, double threshold, double expsim[][], double gosim[][]) throws IOException {
		incorporate(threshold, expsim, gosim);
		preference();
//		clustering();
//		outputfile(filepath1, filepath2, threshold);
//		subsetfile(filepath1, threshold);
	}
	*/
	public AP(List<Point> points, int size){
		top_k = size;
		similar = new double[top_k][top_k];
		r = new double[top_k][top_k];
		a = new double[top_k][top_k];
		idx = new int[top_k];
		constructMatrix(points);
		preference();
		clustering(points);
	}

}

