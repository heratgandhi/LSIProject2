import java.io.*;

public class CorrectMainInputFile {
	public static void main(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("edges1.txt"));
			BufferedReader br1 = new BufferedReader(new FileReader("blocks.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("blockedinput1"));

			String line = "";
			long counter = 0;
			long node = 0;
			String[] parts;
			String dest = "";
			long s,d;
			long currentBlockLimit = Long.parseLong(br1.readLine().trim());
			long currentBlock = 0;
			while ( (line=br.readLine()) != null ) {
				parts = line.split(" ");
				s = Long.parseLong(parts[0]);
				d = Long.parseLong(parts[1]);
				if(s == node) {
					if(counter != 0)
						dest += " " + d;
					else
						dest += d;
					counter ++;
				} else {
					bw.write(currentBlock+"\t"+ node + " " + (1/(double)PageRank.nodes) +" "+counter+" "+dest+"\n");
					while(s != node+1) {
						node++;
						if(node >= currentBlockLimit) {
							currentBlockLimit = Long.parseLong(br1.readLine().trim());
							currentBlock++;
							//System.out.println(currentBlock);
						}
						System.out.println(node);
						bw.write(currentBlock+"\t"+ node +" " + (1/(double)PageRank.nodes) +" 0\n");
					}					
					dest = d+"";
					node = s;
					if(node >= currentBlockLimit) {
						currentBlockLimit = Long.parseLong(br1.readLine().trim());
						currentBlock++;
						//System.out.println(currentBlock);
					}
					counter = 1;
				}
			}
			bw.write(currentBlock+"\t"+ node + " " +(1/(double)PageRank.nodes)+" "+counter+" "+dest+"\n");
			br.close();
			br1.close();
			bw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}