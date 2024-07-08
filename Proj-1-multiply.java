import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Elem implements Writable {

    static final short M_ELEMENT = 0;
    static final short N_ELEMENT = 1;

    short tag;
    int index;
    double value;

    Elem () {}

    Elem(short tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tag = in.readShort();
        this.index = in.readInt();
        this.value = in.readDouble();
    }
}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;
	
    Pair () {}
    Pair ( int i, int j ) { this.i = i; this.j = j; }

    @Override
    public int compareTo(Pair o) {
        if (this.i == o.i) {
            return this.j - o.j;
        }
        return this.i - o.i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.i = in.readInt();
        this.j = in.readInt();
    }

    @Override
    public String toString() {
        return i + "," + j;
    }

    /*...*/
}

public class Multiply extends Configured implements Tool {

    /* ... */
    public static class MatrixMMapper extends Mapper<Object, Text, IntWritable, Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            short tag = Elem.M_ELEMENT;
            int i = s.nextInt();
            System.out.println("Matrix M (i)" + i);
            int j = s.nextInt();
            System.out.println("Matrix M (j)" + j);
            double v = s.nextDouble();
            System.out.println("Matrix M (v)" + v);
            context.write(new IntWritable(j), new Elem(tag, i, v));
            s.close();
        }
    }

    public static class MatrixNMapper extends Mapper<Object, Text, IntWritable, Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            short tag = Elem.N_ELEMENT;
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            context.write(new IntWritable(i), new Elem(tag, j, v));
            s.close();
        }
    }

    public static class ElemReducer extends Reducer<IntWritable, Elem, Pair, DoubleWritable> {

        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                throws IOException, InterruptedException {

            Map<Integer, Double> mapA = new HashMap<>();
            Map<Integer, Double> mapB = new HashMap<>();

            for (Elem value: values) {
                if (value.tag == Elem.M_ELEMENT) {
                    mapA.put(value.index, value.value);
                    System.out.println("key " + key + " index " + value.index + " value for M " + value.value);
                } else if (value.tag == Elem.N_ELEMENT) {
                    mapB.put(value.index, value.value);
                    System.out.println("key " + key + " index " + value.index + " value for N " + value.value);
                }
            }

            double result;
            for (Map.Entry<Integer,Double> entryA : mapA.entrySet()) {
                for (Map.Entry<Integer,Double> entryB : mapB.entrySet()) {
                    result = entryA.getValue() * entryB.getValue();
                    if (entryA.getKey() == 0 && entryB.getKey() == 0) {
                        System.out.println(result);
                        System.out.println(entryA.getValue());
                        System.out.println(entryB.getValue());
                    }
                    context.write(new Pair(entryA.getKey(), entryB.getKey()), new DoubleWritable(result));
                }
            }
        }
    }


    public static class PairMapper extends Mapper<Object, Text, Pair, DoubleWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",|\\t");
            context.write(new Pair(s.nextInt(), s.nextInt()), new DoubleWritable(s.nextDouble()));
        }
    }

    public static class PairReducer extends Reducer<Pair, DoubleWritable, Pair, Text> {

        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                throws IOException, InterruptedException {

            double m = 0;

            for (DoubleWritable value: values) {
                m = m + value.get();
            }

            context.write(null, new Text(key.i + "," + key.j + "," + m));
        }
    }


    @Override
    public int run ( String[] args ) throws Exception {
        /* ... */
        Job job = Job.getInstance();
        job.setJobName("Matrix Pair Multiplication Job");
        job.setJarByClass(Multiply.class);
        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);
        job.setReducerClass(ElemReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, MatrixMMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, MatrixNMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("Matrix Pair Addition Job");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(PairMapper.class);
        job2.setReducerClass(PairReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
        return 0;
    }

    public static void main ( String[] args ) throws Exception {
        ToolRunner.run(new Configuration(),new Multiply(),args);
    }
}
