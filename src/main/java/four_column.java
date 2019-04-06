
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class four_column extends Configured implements Tool {
    // 1、自己的map类
    // 2、继承mapper类，<LongWritable, Text, Text,
    // IntWritable>输入的key,输入的value，输出的key,输出的value
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable MapOutputkey = new IntWritable(1);
        private Text MapOutputValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String strs = value.toString();
            // 分割数据
            String str_four = strs.split(",")[3];
            MapOutputValue.set(str_four);
            System.out.println(str_four);
            context.write(MapOutputValue, MapOutputkey);

        }
    }
    // 2、自己的reduce类，这里的输入就是map方法的输出
    public static class MyReduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        IntWritable countvalue = new IntWritable(1);

        @Override
        // map类的map方法的数据输入到reduce类的group方法中，得到<text,it(1,1)>,再将这个数据输入到reduce方法中
        protected void reduce(Text inputkey, Iterable<IntWritable> inputvalue,
                              Context context) throws IOException, InterruptedException {

            int sum = 0;

            for (IntWritable i : inputvalue) {
                System.out.println(i.get());
                sum = sum + i.get();
            }
            // System.out.println("key: "+inputkey + "...."+sum);
            countvalue.set(sum);
            context.write(inputkey, countvalue);
        }
    }
    // 3运行类，run方法，在测试的时候使用main函数，调用这个类的run方法来运行

    /**
     * param args 参数是接受main方得到的参数，在run中使用
     */
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(this.getConf(), "four_column");
        // set mainclass
        job.setJarByClass(four_column.class);
        // set mapper
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // set reducer
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set path
        Path inpath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);
        FileSystem fs = outpath.getFileSystem(conf);
       // FileSystem fs = FileSystem.get(conf);
        // 存在路径就删除
        if (fs.exists(outpath)) {
            System.out.println("删除已经存在的输出路径");
            fs.delete(outpath, true);
        }
        job.setNumReduceTasks(1);


        System.out.println("到这里都没有问题");
        boolean status = job.waitForCompletion(true);

        if (!status) {
            System.err.println("the job is error!!");
        }

        return status ? 0 : 1;

    }
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        for (int i = 0; i < args.length; i++) {
            System.out.println("参数:" + args[i]);
        }

        Configuration conf = new Configuration();



        int atatus;
        try {
            atatus = ToolRunner.run(conf, new four_column(), args);
            System.exit(atatus);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

