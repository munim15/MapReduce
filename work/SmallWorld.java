
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Example writable type
    public static class Evalue implements Writable {

        public long[] exampleLongArray; //example array of longs
	public Text friends; //Text object containing your direct friends
        public HashMap<Long, Long> dist; //(Source, Distance from Source)
        
	public Evalue(Text friends, HashMap<Long, Long> dist) {
	    this.friends = friends;
	    this.dist = dist;
        }

        public Evalue() {
	    this.friends = new Text();
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
	    friends.write(out);
            int numkeys = 0;
            if(dist != null) {
                numkeys = dist.keySet().size();
            }
            out.writeInt(numkeys);
            if(dist != null && !dist.isEmpty()) {
                Set<Long> arr = dist.keySet();
                for(long a : arr) {
                    out.writeLong(a);
                    out.writeLong((long) dist.get(a));
                }
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            friends.readFields(in);
            int numkeys = in.readInt();
            dist = new HashMap<Long, Long>();
            for(int i = 0; i < numkeys; i ++) {
                long key = (long) in.readLong();
                long value = (long) in.readLong();
                dist.put(key, value);
            }
        }

        public String toString() {
	    return friends.toString() + " | " + dist.toString(); 
        }
    }


    /* The first mapper. Part of the graph loading process.*/
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            context.write(key, value);
	    context.write(value, new LongWritable(Long.MAX_VALUE));
        }
    }


    /* The first reducer. 
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, Evalue> {

        public long denom;

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));

            // You can print it out by uncommenting the following line:
            // System.out.println(denom);

            // Example of iterating through an Iterable
            HashMap<Long, Long> dist = new HashMap<Long, Long>();
	    String friends = "";
            if (Math.random() < 1.0/denom) {
                dist.put(key.get(), 0L);
            }
            
            for (LongWritable value : values) {
		if(value.get() != Long.MAX_VALUE) { 
		    friends += value.get() + " ";
		}
            }
            context.write(key, new Evalue(new Text(friends), dist));
        }

    }


    // ------- additional Mappers and Reducers Here ------- //
    
    public static class BFSMap extends Mapper<LongWritable, Evalue,
                       LongWritable, Evalue> {

        
    @Override
        public void map(LongWritable key, Evalue value, Context context)
        throws IOException, InterruptedException {
        long check = Long.parseLong(context.getConfiguration().get("count"));
        Set<Long> starts = value.dist.keySet();
        boolean f = false;
        for (long x : starts) {
        if (value.dist.get(x) == check) {
            f = true;
        }
        }        
            if(f) {
                String s = value.friends.toString();
                Scanner inp = new Scanner(s);
                while (inp.hasNextLong()) {
		    long friend = inp.nextLong();
                    HashMap<Long, Long> ndist =
			new HashMap<Long, Long>(value.dist);
                    Set<Long> sources = ndist.keySet();
                    for(long source : sources) {
                        //LongWritable sourcex= new Longwritable(source.get());
                        ndist.put(source, (ndist.get(source) + 1L));
                    }
                    context.write(new LongWritable(friend),
				  new Evalue(new Text(""), ndist));
                }
            }
            context.write(key, value);
        }
    }
    public static class BFSReduce extends Reducer<LongWritable, Evalue, LongWritable, Evalue> {
    @Override
            public void reduce(LongWritable key, Iterable<Evalue> values, Context context) 
            throws IOException, InterruptedException {
                String friendsx = "";
                HashMap<Long, Long> ndist = new HashMap<Long, Long>();
                for (Evalue value : values) {
                    friendsx += value.friends.toString();
                    if (!value.dist.isEmpty()) {
                       Set<Long> sources = value.dist.keySet();
                       for (long source : sources) {
                           long pathlength = value.dist.get(source);
                           if (ndist.containsKey(source)) {
                               long min = (long) Math.min(pathlength, ndist.get(source));
                               ndist.put(source, min);
                            } else {
                               ndist.put(source, pathlength);
                            }
                        }
                    }
                }
                context.write(key, new Evalue(new Text(friendsx), ndist));
            }
        }

    public static class HistMap extends Mapper<LongWritable, Evalue, LongWritable, LongWritable> {
            
            @Override
            public void map(LongWritable key, Evalue value, Context context)
            throws IOException, InterruptedException{
                Set<Long> sources = value.dist.keySet();
                for (long source : sources) {
                    context.write(new LongWritable((long) value.dist.get(source)), new LongWritable(1L));
                }
            }
        }

    public static class HistReduce extends Reducer
        <LongWritable, LongWritable, LongWritable, LongWritable> {
            
            @Override
            public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
                long counter = 0;
                for (LongWritable value : values) {
                    counter += value.get();
                }
                context.write(new LongWritable((long) key.get()), new LongWritable(counter));
            }
    }















    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        //conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");
	job.setNumReduceTasks(48);
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Evalue.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);
     
        //Repeats BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
	    conf.set("count", (new Long(i)).toString());
            job = new Job(conf, "bfs" + i);
	    job.setNumReduceTasks(48);
            job.setJarByClass(SmallWorld.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Evalue.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Evalue.class);

            job.setMapperClass(BFSMap.class); // currently the default Mapper
            job.setReducerClass(BFSReduce.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
	job.setNumReduceTasks(1);
        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(HistMap.class); // currently the default Mapper
        job.setReducerClass(HistReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
