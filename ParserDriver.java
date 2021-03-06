import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
*
* @author root
*/
public class ParserDriver {

/**
* @param args the command line arguments
*/
public static void main(String[] args) {
try {
runJob(args);

} catch (IOException ex) {
Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE, null, ex);
}

}


public static void runJob(String [] args ) throws IOException {

Configuration conf = new Configuration();

conf.set("xmlinput.start", "<page>");
conf.set("xmlinput.end", "</page>");
conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

Job job = new Job(conf, "jobName");

FileInputFormat.setInputPaths(job, args[0]);
job.setJarByClass(ParserDriver.class);
job.setMapperClass(MyParserMapper.class);
job.setNumReduceTasks(0);
job.setInputFormatClass(XmlInputFormat.class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);
Path outPath = new Path(args[1]);
FileOutputFormat.setOutputPath(job, outPath);
FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
if (dfs.exists(outPath)) {
dfs.delete(outPath, true);
}


try {

job.waitForCompletion(true);

   } catch (InterruptedException ex) {
     Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE, null, ex);
   } catch (ClassNotFoundException ex) {
     Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE, null, ex);
  }

}

}