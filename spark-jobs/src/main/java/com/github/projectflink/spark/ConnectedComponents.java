package com.github.projectflink.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConnectedComponents {
	public static void main(String args[]) {
		if(!parseParameters(args)) {
			return;
		}

		SparkConf conf = new SparkConf().setAppName("Connected Components").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaPairRDD<Integer, Integer> edges = sc
			.textFile(edgesPath)
			.flatMapToPair(new UndirectedEdge())
			.cache();
		
		JavaPairRDD<Integer, Integer> cc = edges
			.groupByKey()
			.mapToPair(new PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, Integer>() {
				@Override
				public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
					return new Tuple2<Integer, Integer>(t._1(), t._1());
				}
			});

		JavaPairRDD<Integer, Integer> result = cc.cache();


		for (int i = 0; i < maxIterations; i++) {
			cc = cc.join(edges)
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>, Integer, Integer>() {
					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> t) throws Exception {
						return new Tuple2<Integer, Integer>(t._2()._2(), t._2()._1());
					}
				})
				.reduceByKey(new SmallestNeighbor())
				.join(result)
				.filter(new LessThanCurrent())
				.mapToPair(new NextRoundUpdate());

			result = result.cogroup(cc)
				.mapToPair(new UpdateResult());

			if (cc.count() == 0) {
				break;
			}
		}

		result.saveAsTextFile(outputPath);

	}

	public static final class UndirectedEdge implements PairFlatMapFunction<String, Integer, Integer> {
		@Override
		public Iterable<Tuple2<Integer, Integer>> call(String s) throws Exception {
			String [] line = s.split(" ");
			Integer v1 = Integer.parseInt(line[0]);
			Integer v2 = Integer.parseInt(line[1]);
			List<Tuple2<Integer, Integer>> result = new ArrayList<Tuple2<Integer, Integer>>();
			result.add(new Tuple2<Integer, Integer>(v1, v2));
			result.add(new Tuple2<Integer, Integer>(v2, v1));
			return result;
		}
	}

	public static final class SmallestNeighbor implements Function2<Integer, Integer, Integer> {
		@Override
		public Integer call(Integer t1, Integer t2) throws Exception {
			return Math.min(t1, t2);
		}
	}

	public static final class LessThanCurrent implements Function<Tuple2<Integer, Tuple2<Integer, Integer>>, Boolean> {
		@Override
		public Boolean call(Tuple2<Integer, Tuple2<Integer, Integer>> t) throws Exception {
			return t._2()._1().compareTo(t._2()._2()) < 0;
		}
	}

	public static final class NextRoundUpdate implements PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>, Integer, Integer> {
		@Override
		public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Integer, Integer>> t) throws Exception {
			return new Tuple2<Integer, Integer>(t._1(), t._2()._1());
		}
	}

	public static final class UpdateResult implements PairFunction<Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>>, Integer, Integer> {
		@Override
		public Tuple2<Integer, Integer> call(Tuple2<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> t) throws Exception {
			Iterator<Integer> t1 = t._2()._1().iterator();
			Iterator<Integer> t2 = t._2()._2().iterator();
			if (t2.hasNext()) {
				return new Tuple2<Integer, Integer>(t._1(), Math.min(t1.next(), t2.next()));
			} else {
				return new Tuple2<Integer, Integer>(t._1(), t1.next());
			}
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String master = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;


	private static boolean parseParameters(String[] programArguments) {
		// parse input arguments
		if(programArguments.length == 4) {
			master = programArguments[0];
			edgesPath = programArguments[1];
			outputPath = programArguments[2];
			maxIterations = Integer.parseInt(programArguments[3]);
		} else {
			System.err.println("Usage: Connected Components <master> <edges path> <result path> <max iteration>");
			return false;
		}
		return true;
	}
}
