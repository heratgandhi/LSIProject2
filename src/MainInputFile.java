import java.io.*;

public class MainInputFile {
	public static void main(String[] args) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("edges1.txt"));
			BufferedReader br1 = new BufferedReader(new FileReader("blocks.txt"));
			BufferedWriter bw = new BufferedWriter(new FileWriter("blocked_input"));
			
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
					dest = d+"";
					node++;
					if(node == currentBlockLimit) {
						currentBlockLimit = Long.parseLong(br1.readLine().trim());
						currentBlock++;
					}
					counter = 1;
				}
			}
			bw.write(currentBlock+"\t"+ node + " " +(1/(double)PageRank.nodes)+" "+counter+" "+dest+"\n");
			br.close();
			bw.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
