package cn.andy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class TopN {

    public static class TopTenMapper extends
            Mapper<Object, Text, NullWritable, IntWritable> {

        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        @Override
        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            int N = 10; //默认为Top10
            N = Integer.parseInt(context.getConfiguration().get("N"));

            String[] values = line.toString().split(" ");
            for(String value : values){
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    repToRecordMap.put(Integer.parseInt(itr.nextToken()), " ");
                    if (repToRecordMap.size() > N) {
                        repToRecordMap.remove(repToRecordMap.firstKey());
                    }
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer i : repToRecordMap.keySet()) {
                try {
                    context.write(NullWritable.get(), new IntWritable(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class TopTenReducer extends
            Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        @Override
        protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int N = 10; //默认为Top10
            N = Integer.parseInt(context.getConfiguration().get("N"));
            for (IntWritable value : values) {
                repToRecordMap.put(value.get(), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            for (Integer i : repToRecordMap.descendingMap().keySet()) {
                context.write(NullWritable.get(), new IntWritable(i));
            }
        }
    }



    public static void main(String[] args){
        /*if (args.length != 3) {
            throw new IllegalArgumentException(
                    "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: hadoop jar <jar-name> "
                            + "TopN.TopN "
                            + "<the value of N>"
                            + "<input-path> "
                            + "<output-path>");
        }*/
        Configuration conf = new Configuration();
        conf.set("N", "10");
        try {
            Job job = Job.getInstance(conf, "TopN");
            job.setJobName("TopN");
            Path inputPath = new Path("D:\\idea_project\\bigdata\\topN\\src\\resources\\topN.txt");
            Path outputPath = new Path("D:\\idea_project\\bigdata\\topN\\src\\resources\\topN_result.txt");
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setJarByClass(TopN.class);
            job.setMapperClass(TopTenMapper.class);
            job.setReducerClass(TopTenReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(NullWritable.class);// map阶段的输出的key
            job.setMapOutputValueClass(IntWritable.class);// map阶段的输出的value

            job.setOutputKeyClass(NullWritable.class);// reduce阶段的输出的key
            job.setOutputValueClass(IntWritable.class);// reduce阶段的输出的value

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
        }
    }


}
