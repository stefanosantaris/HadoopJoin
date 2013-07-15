package HadoopJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The ThreeWayJoin class, which implements the hadoop Map/Reduce implementation
 * of acyclicaly joining three relations, designed to be able to perform an
 * optimized single pass join of the relations in question.
 */
public class ThreeWayJoin {

	/**
	 * The parameters needed to optimize the three way chained join.
	 */
	public static int k, r, s, t, b, c;

	/**
	 * The Mapper class implementation of the ThreeWayJoin class.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * Emits, for each record read, a tuple with a <i>key element</i> with a
		 * value of the form <i>Text("Key")</i> and a <i>value element</i> with
		 * a value of the form <i>Text("Value")</i>.
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
		 *      java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Contains the b parameter which is calculated by the Math.sqrt(k *
			// t / r) on HadoopJoin.java
			b = Integer.parseInt(context.getConfiguration().get("b"));
			// Contains the c parameter which is calculated by the Math.sqrt(k *
			// r / t) on HadoopJoin.java
			c = Integer.parseInt(context.getConfiguration().get("c"));
			String[] line = value.toString().split(" ");
			if (line.length < 3) {
				return;
			}
			// for each context write we assume the relation's letter as the
			// first parameter followed by the appropriate value of the tuple in
			// order the results to be sorted according the relation's name to
			// the reducers
			if (line[0].equals("R")) {
				// send the value in c reducers
				for (int i = 0; i < c; i++) {
					context.write(
							new Text("R ".concat(line[2]).concat(" " + i)),
							value);
				}
			} else if (line[0].equals("S")) {
				// send the value in one reducer
				context.write(
						new Text("S ".concat(line[1]).concat(" ")
								.concat(line[2])), value);
			} else if (line[0].equals("T")) {
				// send the value in b reducers
				for (int i = 0; i < b; i++) {
					context.write(
							new Text("T ".concat(i + " ").concat(line[1])),
							value);
				}
			}
		}
	}

	/**
	 * The Partitioner class implementation of the ThreeWayJoin class which
	 * performs the ThreeWayJoin optimization by means of replicating the same
	 * (Key, Value) tuples to multiple reducers in order to optimize the
	 * grouping of records and the concurrency of the join operation.
	 */
	public static class Partition extends Partitioner<Text, Text> {

		/**
		 * Chooses the partition for each (Key, Value) tuple which best serves
		 * the the ThreeWayJoin optimization given the number of reducers
		 * <i>k</i> and by computing the replication values <i>b</i> and
		 * <i>c</i> for attributes <i>B</i> and <i>C</i> respectively. The
		 * constraint regargind the values <i>b</i> and <i>c</i> is that
		 * <i>b*c=k</i> must hold.
		 * 
		 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object,
		 *      java.lang.Object, int)
		 */
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] keyParts = key.toString().split(" ");
			int y = 0, x = 0;
			if (keyParts[0].equals("R")) {
				// module b according the rows of the reducers table
				y = (int) (Long.parseLong(keyParts[1]) % b);
				x = (int) (Long.parseLong(keyParts[2]));
			} else if (keyParts[0].equals("S")) {
				y = (int) (Long.parseLong(keyParts[1]) % b);
				x = (int) (Long.parseLong(keyParts[2]) % c);
			} else if (keyParts[0].equals("T")) {
				y = (int) (Long.parseLong(keyParts[1]));
				// module c according the columns of the reducers table
				x = (int) (Long.parseLong(keyParts[2]) % c);
			}
			return (getReducerId(y, x));
		}

		/**
		 * Gets the reducer id for the (Key, Value) tuple being processed.
		 * 
		 * @param y
		 *            the y
		 * @param x
		 *            the x
		 * @return the reducer id
		 */
		private static int getReducerId(int y, int x) {
			return (y * c) + x;
		}
	}

	/**
	 * The Reducer class implementation of the ThreeWayJoin class.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		/**
		 * The HashMap objects holding the intermediate values of relations R
		 * and S which are to be joined with the values of relation T.
		 */
		private HashMap<String, List<String>> RMap, SMap;

		/**
		 * Instantiates a new Reduce object.
		 */
		public Reduce() {
			RMap = new HashMap<String, List<String>>();
			SMap = new HashMap<String, List<String>>();
		}

		/**
		 * Receives <i>(Key, Value)</i> tuples of the form <i>(Text("Key"),
		 * [Text("Value"), ...])</i>.
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
		 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIterator = values.iterator();
			while (valuesIterator.hasNext()) {
				String[] value = valuesIterator.next().toString().split(" ");
				if (value[0].equals("R")) {
					String a = value[1].toString();
					String b = value[2].toString();
					// Insert into RMap every tuple that contains R as a key.
					// Values
					// will be arrived in sorted order according to the
					// relation's name
					if (!RMap.containsKey(b)) {
						List<String> aList = new ArrayList<String>();
						aList.add(a);
						RMap.put(b, aList);
					} else {
						RMap.get(b).add(a);
					}
				} else if (value[0].equals("S")) {
					String b = value[1].toString();
					String c = value[2].toString();
					// Insert into SMap every tuple that equalizes to any RMap
					// value
					if (RMap.containsKey(b)) {
						if (!SMap.containsKey(c)) {
							List<String> bList = new ArrayList<String>();
							bList.add(b);
							SMap.put(c, bList);
						} else {
							SMap.get(c).add(b);
						}
					}
					// Output values whenever a T tuple arrives
				} else if (value[0].equals("T")) {
					String c = value[1].toString();
					String d = value[2].toString();
					if (SMap.containsKey(c)) {
						List<String> bList = SMap.get(c);
						for (String b : bList) {
							List<String> aList = RMap.get(b);
							for (String a : aList) {
								context.write(
										new Text("R|><|S|><|T"),
										new Text(a.concat(" ").concat(b)
												.concat(" ").concat(c)
												.concat(" ").concat(d)));
							}
						}
					}
				}
			}
		}
	}
}