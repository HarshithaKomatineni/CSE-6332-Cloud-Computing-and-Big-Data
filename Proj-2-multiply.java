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
return "Pair{" +
"i=" + i +
", j=" + j +
'}';
}
/*...*/
}
public class Multiply extends Configured implements Tool {
public static final int MATRIX_N_COLUMNS = 3;
public static final int MATRIX_M_ROWS = 4;
public static final int MATRIX_M_COLUMNS = 3;
/* ... */
public static class MatrixMMapper extends Mapper<Object, Text, Pair, Elem> {
@Override
public void map ( Object key, Text value, Context context )
throws IOException, InterruptedException {
Scanner s = new Scanner(value.toString()).useDelimiter(",");
short tag = Elem.M_ELEMENT;
int i = s.nextInt();
int j = s.nextInt();
double v = s.nextDouble();
for (int k = 0; k < MATRIX_N_COLUMNS; k++) {
context.write(new Pair(i, k), new Elem(tag, j, v));
}
s.close();
}
}
public static class MatrixNMapper extends Mapper<Object, Text, Pair, Elem> {
@Override
public void map ( Object key, Text value, Context context )
throws IOException, InterruptedException {
Scanner s = new Scanner(value.toString()).useDelimiter(",");
short tag = Elem.N_ELEMENT;
int j = s.nextInt();
int k = s.nextInt();
double v = s.nextDouble();
for (int i = 0; i < MATRIX_M_ROWS; i++) {
System.out.println("Got the value ");
context.write(new Pair(i, k), new Elem(tag, j, v));
}
s.close();
}
}
public static class PairElemReducer extends Reducer<Pair, Elem, Pair, Text> {
@Override
public void reduce ( Pair key, Iterable<Elem> values, Context context )
throws IOException, InterruptedException {
Map<Integer, Double> mapA = new HashMap<>();
Map<Integer, Double> mapB = new HashMap<>();
for (Elem value: values) {
if (value.tag == Elem.M_ELEMENT) {
mapA.put(value.index, value.value);
} else if (value.tag == Elem.N_ELEMENT) {
mapB.put(value.index, value.value);
}
}
double result = 0.0;
for (int j = 0; j < MATRIX_M_COLUMNS; j++) {
result += ((mapA.get(j) != null ? mapA.get(j) : 0) *
(mapB.get(j) != null ? mapB.get(j) : 0));
}
context.write(null, new Text(key.i + "," + key.j + "," + result));
}
}
@Override
public int run ( String[] args ) throws Exception {
/* ... */
Job job = Job.getInstance();
job.setJobName("Matrix Pair Multiplication Job");
job.setJarByClass(Multiply.class);
job.setOutputKeyClass(Pair.class);
job.setOutputValueClass(Text.class);
job.setMapOutputKeyClass(Pair.class);
job.setMapOutputValueClass(Elem.class);
job.setReducerClass(PairElemReducer.class);
job.setOutputFormatClass(TextOutputFormat.class);
MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,
MatrixMMapper.class);
MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,
MatrixNMapper.class);
FileOutputFormat.setOutputPath(job,new Path(args[2]));
job.waitForCompletion(true);
return 0;
}
public static void main ( String[] args ) throws Exception {
ToolRunner.run(new Configuration(),new Multiply(),args);
}
}
