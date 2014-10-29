/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.github.projectflink.testPlan;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Iterator;

@SuppressWarnings("serial")
public class ConnectedComponentsBulk {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String... args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Integer, Integer>> edges = env
			.readCsvFile(edgesPath)
			.fieldDelimiter(' ')
			.types(Integer.class, Integer.class)
			.flatMap(new UndirectEdge());

		DataSet<Tuple2<Integer, Integer>> edges2 = env
			.readCsvFile(edgesPath)
			.fieldDelimiter(' ')
			.types(Integer.class, Integer.class)
			.flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Integer, Integer>> verticesWithInitialId = edges2
			.groupBy(0)
			.reduceGroup(new InitialValue());

		IterativeDataSet<Tuple2<Integer, Integer>> solutionSet = verticesWithInitialId.iterate(maxIterations);
		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Integer, Integer>> delta = solutionSet.join(edges, JoinOperatorBase.JoinHint.REPARTITION_HASH_FIRST).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
			.groupBy(0).aggregate(Aggregations.MIN, 1)
			.join(solutionSet).where(0).equalTo(0)
			.with(new ComponentIdFilter());

		DataSet<Tuple2<Integer, Integer>> newSolutionSet = solutionSet.coGroup(delta).where(0).equalTo(0).with(new UpdateSolutionSet());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Integer, Integer>> result = solutionSet.closeWith(newSolutionSet, delta);

		// emit result
		result.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("Connected Components Bulk Iteration");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Function that initial the connected components with its own id.
	 */
	public static final class InitialValue implements GroupReduceFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<Integer, Integer>> t, Collector<Tuple2<Integer, Integer>> c) throws Exception {
			Integer v = t.iterator().next().f0;
			c.collect(new Tuple2<Integer, Integer>(v, v));
		}
	}

	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		Tuple2<Integer, Integer> invertedEdge = new Tuple2<Integer, Integer>();

		@Override
		public void flatMap(Tuple2<Integer, Integer> edge, Collector<Tuple2<Integer, Integer>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
	}

	/**
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that
	 * a vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
	 * produces a (Target-vertex-ID, Component-ID) pair.
	 */
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public Tuple2<Integer, Integer> join(Tuple2<Integer, Integer> vertexWithComponent, Tuple2<Integer, Integer> edge) {
			return new Tuple2<Integer, Integer>(edge.f1, vertexWithComponent.f1);
		}
	}



	@ConstantFieldsFirst("0")
	public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void join(Tuple2<Integer, Integer> candidate, Tuple2<Integer, Integer> old, Collector<Tuple2<Integer, Integer>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	public static final class UpdateSolutionSet implements CoGroupFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

		@Override
		public void coGroup(Iterable<Tuple2<Integer, Integer>> ss, Iterable<Tuple2<Integer, Integer>> delta, Collector<Tuple2<Integer, Integer>> c) throws Exception {
			Iterator<Tuple2<Integer, Integer>> it1 = ss.iterator();
			Iterator<Tuple2<Integer, Integer>> it2 = delta.iterator();
			if (it2.hasNext()) {
				c.collect(it2.next());
			} else {
				c.collect(it1.next());
			}
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		// parse input arguments
		if(programArguments.length == 3) {
			edgesPath = programArguments[0];
			outputPath = programArguments[1];
			maxIterations = Integer.parseInt(programArguments[2]);
			return true;
		} else {
			System.err.println("Usage: ConnectedComponents <edges path> <result path> <max number of iterations>");
			return false;
		}
	}
}

