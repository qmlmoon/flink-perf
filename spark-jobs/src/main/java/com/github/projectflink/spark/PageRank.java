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

public class PageRank {

	private static final double DAMPENING_FACTOR = 0.85;


	public static void main(String[] args) {

		if(!parseParameters(args)) {
			return;
		}

		SparkConf conf = new SparkConf().setAppName("Page Rank").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		// get input data
		JavaPairRDD<Integer, Integer> linksInput = sc
			.textFile(linksInputPath)
			.mapToPair(new PairFunction<String, Integer, Integer>() {
				@Override
				public Tuple2<Integer, Integer> call(String s) throws Exception {
					String [] line = s.split(" ");
					return new Tuple2<Integer, Integer>(Integer.parseInt(line[0]), Integer.parseInt(line[1]));
				}
			});

		// build adjacency list from link input
		JavaPairRDD<Integer, Integer[]> adjacencyListInput = linksInput
			.groupByKey(200)
			.mapValues(new Function<Iterable<Integer>, Integer[]>() {
				@Override
				public Integer[] call(Iterable<Integer> t) throws Exception {
					ArrayList<Integer> link = new ArrayList<Integer>();
					Iterator<Integer> it = t.iterator();
					while (it.hasNext()) {
						link.add(it.next());
					}
					return link.toArray(new Integer[link.size()]);
				}
			}).cache();

		// assign initial rank to pages
		JavaPairRDD<Integer, Double> pagesWithRanks = adjacencyListInput.mapValues(new RankAssigner(numPages));

		for (int i = 0; i < maxIterations; i++) {
			// join pages with outgoing edges and distribute rank
			JavaPairRDD<Integer, Double> rankToDistribute = pagesWithRanks
				.join(adjacencyListInput)
				.values()
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Double, Integer[]>, Integer, Double>() {
					@Override
					public Iterable<Tuple2<Integer, Double>> call(Tuple2<Double, Integer[]> t) throws Exception {
						List<Tuple2<Integer, Double>> rankDelta = new ArrayList<Tuple2<Integer, Double>>();
						int urlCount = t._2().length;

						double r = t._1() / urlCount;

						for (Integer s : t._2()) {
							rankDelta.add(new Tuple2<Integer, Double>(s, r));
						}
						return rankDelta;
					}
				});

			// sum ranks and apply dampening factor
			pagesWithRanks = rankToDistribute
				.reduceByKey(new Function2<Double, Double, Double>() {
					@Override
					public Double call(Double t1, Double t2) throws Exception {
						return t1 + t2;
					}
				})
				.mapToPair(new Dampener(DAMPENING_FACTOR, numPages));
		}

		pagesWithRanks.saveAsTextFile(outputPath);

	}

	public static final class RankAssigner implements Function<Integer[], Double> {

		private double rank;

		public RankAssigner(Integer numVertices){
			this.rank = 1.0d / numVertices;
		}

		@Override
		public Double call(Integer[] a) throws Exception {
			return rank;
		}
	}

	public static final class Dampener implements PairFunction<Tuple2<Integer, Double>, Integer, Double> {

		private final double dampening;
		private final double randomJump;

		public Dampener(double dampening, Integer numVertices) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numVertices;
		}
		@Override
		public Tuple2<Integer, Double> call(Tuple2<Integer, Double> t) throws Exception {
			return new Tuple2<Integer, Double>(t._1(), t._2() * dampening + randomJump);
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************


	private static String master;
	private static String linksInputPath;
	private static String outputPath;
	private static int numPages;
	private static int maxIterations;

	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length == 5) {
			master = programArguments[0];
			linksInputPath = programArguments[1];
			outputPath = programArguments[2];
			numPages = Integer.parseInt(programArguments[3]);
			maxIterations = Integer.parseInt(programArguments[4]);
		} else {
			System.err.println("Usage: PageRankBasic <master> <links path> <output path> <num pages> <num iterations>");
			return false;
		}
		return true;
	}
}
