import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MatrixMul {
public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
        	Configuration conf = context.getConfiguration();
        	
        	 Text Pos = new Text();
             Text Value = new Text();
             
            int d = Integer.parseInt(conf.get("d"));
            int p = Integer.parseInt(conf.get("p"));
            int s = Integer.parseInt(conf.get("s"));
            int u = Integer.parseInt(conf.get("u"));
            int v = Integer.parseInt(conf.get("v"));
            
            int ABlock = d/s; 
            int BBlock = p/v; 
            
            String str = value.toString();
       	    String[] arr = str.split(",");
       	
            
            int i = Integer.parseInt(arr[1]);
            int j = Integer.parseInt(arr[2]);
            int x = Integer.parseInt(arr[1]);
            int k = Integer.parseInt(arr[2]);
            
           
            if (arr[0].equals("A")) {
                
                for (int kV = 0; kV < BBlock; kV++) {
                	Pos.set(Integer.toString(i/s) + "," + Integer.toString(j/u) + "," + Integer.toString(kV));
                    Value.set("A," + Integer.toString(i%s) + "," + Integer.toString(j%u) + "," + arr[3]);
                    context.write(Pos, Value);
			 }
            } else {
                
                for (int iS = 0; iS < ABlock; iS++) {
                	Pos.set(Integer.toString(iS) + "," + Integer.toString(x/u) + "," + Integer.toString(k/v));
                     Value.set("B," + Integer.toString(x%u) + "," + Integer.toString(k%v) + "," + arr[3]);
                     context.write(Pos, Value);
                 }
             }
         }
     }
public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	Text outputValue = new Text();
        	
        	String[] arrindex;
            String[] iModSAndJModT;
            String[] jModTAndKModV;
            
            String hash_key;
            String[] Indices = key.toString().split(",");
            String[] arrindices;
            
          
            int w = Integer.parseInt(conf.get("s"));
            int q = Integer.parseInt(conf.get("v"));
                    
            ArrayList<Entry<String, Float>> listC = new ArrayList<Entry<String, Float>>();
            ArrayList<Entry<String, Float>> listD = new ArrayList<Entry<String, Float>>();
            
            for (Text val : values) {
            	arrindex = val.toString().split(",");
                    if (arrindex[0].equals("A")) {
                    listC.add(new SimpleEntry<String, Float>(arrindex[1] + "," + arrindex[2], Float.parseFloat(arrindex[3])));
                } else {
                    listD.add(new SimpleEntry<String, Float>(arrindex[1] + "," + arrindex[2], Float.parseFloat(arrindex[3])));
                }
            }
            
            HashMap<String, Float> hash = new HashMap<String, Float>();
            
            	for (Entry<String, Float> a : listC) {
                iModSAndJModT = a.getKey().split(",");
               float x_ij = a.getValue();
                for (Entry<String, Float> b : listD) {
                    jModTAndKModV = b.getKey().split(",");
                 float y_jk = b.getValue();
                    if (iModSAndJModT[1].equals(jModTAndKModV[0])) {
                    	hash_key = iModSAndJModT[0] + "," + jModTAndKModV[1];
                        if (hash.containsKey(hash_key)) {
                             hash.put(hash_key, hash.get(hash_key) + x_ij*y_jk);
                        } else {
                            hash.put(hash_key, x_ij*y_jk);
                        }
                    }
                }
            }
        
            for (Entry<String,Float> enter : hash.entrySet()) {
               arrindices = enter.getKey().split(",");
              String abc = Integer.toString(Integer.parseInt(Indices[0])*w + Integer.parseInt(arrindices[0]));
              String xyz = Integer.toString(Integer.parseInt(Indices[2])*q + Integer.parseInt(arrindices[1]));
               outputValue.set(abc + "," + xyz + "," + Float.toString(enter.getValue()));
               context.write(null, outputValue);
            }
        }
    }
         public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        conf.set("d", "2");
        conf.set("n", "5");
        conf.set("p", "3");
        conf.set("s", "2"); 
        conf.set("u", "5"); 
        conf.set("v", "3"); 
        Job job = new Job(conf, "Multiplication");
        job.setJarByClass(MatrixMul.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath( job, new Path("input")); 
        FileOutputFormat.setOutputPath( job, new Path("output")); 
 
        job.waitForCompletion(true);
    }
}

