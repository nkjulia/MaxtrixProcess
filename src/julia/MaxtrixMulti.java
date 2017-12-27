package julia;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxtrixMulti extends Configured implements Tool {
	private static String tag; // current matrix
	private static int crow = 2;// 矩阵A的行数
	private static int ccol = 2;// 矩阵B的列数
	private static int arow = 0; // current arow
	private static int brow = 0; // current brow

	public static String tag_a = "a";
	public static String tag_b = "b";
    MaxtrixMulti(int a_row,int b_col){
    	crow = a_row;
    	ccol = b_col;
    }
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void setup(Context ctx) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) ctx.getInputSplit();
			tag = split.getPath().getName();
			System.out.println("Get:tag:" + tag);
		}

		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
			StringTokenizer str = new StringTokenizer(value.toString());
			if (tag_a.equals(tag)) { // left matrix,output key:x,y
				int col = 0;
				while (str.hasMoreTokens()) {
					String item = str.nextToken(); // current x,y = line,col
					for (int i = 0; i < ccol; i++) {
						Text outkey = new Text(arow + "," + i);
						Text outvalue = new Text(tag_a +"," + col + "," + item);
						ctx.write(outkey, outvalue);
						System.out.println(outkey + " | " + outvalue);
					}
					col++;
				}
				arow++;

			} else if (tag_b.equals(tag)) {
				int col = 0;
				while (str.hasMoreTokens()) {
					String item = str.nextToken(); // current x,y = line,col
					for (int i = 0; i < crow; i++) {
						Text outkey = new Text(i + "," + col);
						Text outvalue = new Text(tag_b + ","+ brow + "," + item);
						ctx.write(outkey, outvalue);
						System.out.println(outkey + " | " + outvalue);
					}
					col++;
				}
				brow++;

			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
			Map<String, String> matrixa = new HashMap<String, String>();
			Map<String, String> matrixb = new HashMap<String, String>();

			for (Text val : values) { // values example : b,0,2 or a,0,4
				StringTokenizer str = new StringTokenizer(val.toString(), ",");
				String sourceMatrix = str.nextToken();
				if (tag_a.equals(sourceMatrix)) {
					matrixa.put(str.nextToken(), str.nextToken()); // (0,4)
				}
				if (tag_b.equals(sourceMatrix)) {
					matrixb.put(str.nextToken(), str.nextToken()); // (0,2)
				}
			}

			int result = 0;
			Iterator<String> iter = matrixa.keySet().iterator();
			while (iter.hasNext()) {
				String mapkey = iter.next();
				result += Integer.parseInt(matrixa.get(mapkey)) * Integer.parseInt(matrixb.get(mapkey));
			}
            System.out.println(Integer.toString(result));
			ctx.write(key, new Text(String.valueOf(result)));
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = Job.getInstance(getConf());
		job.setJarByClass(julia.MaxtrixMulti.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(arg0[0] + tag_a));
		FileInputFormat.addInputPath(job, new Path(arg0[0] + tag_b));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		// TODO Auto-generated method stub
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new julia.MaxtrixMulti(Integer.parseInt(args[2]),Integer.parseInt(args[3])), args);
		System.exit(res);
	}
}
