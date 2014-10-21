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

		// read vertex and edge data
		DataSet<Long> vertices = env
			.readCsvFile(verticesPath)
			.types(Long.class)
			.map(
				new MapFunction<Tuple1<Long>, Long>() {
					public Long map(Tuple1<Long> value) { return value.f0; }
				});

		DataSet<Tuple2<Long, Long>> edges = env
			.readCsvFile(edgesPath)
			.fieldDelimiter(' ')
			.types(Long.class, Long.class)
			.flatMap(new UndirectEdge());

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices.map(new DuplicateValue<Long>());

		IterativeDataSet<Tuple2<Long, Long>> solutionSet = verticesWithInitialId.iterate(maxIterations);
		// apply the step logic: join with the edges, select the minimum neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> delta = solutionSet.join(edges).where(0).equalTo(0).with(new NeighborWithComponentIDJoin())
			.groupBy(0).aggregate(Aggregations.MIN, 1)
			.join(solutionSet).where(0).equalTo(0)
			.with(new ComponentIdFilter());

		DataSet<Tuple2<Long, Long>> newSolutionSet = solutionSet.coGroup(delta).where(0).equalTo(0).with(new UpdateSolutionSet());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> result = solutionSet.closeWith(newSolutionSet, delta);

		// emit result
		result.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("Connected Components Bulk Iteration");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Function that turns a value into a 2-tuple where both fields are that value.
	 */
	@ConstantFields("0 -> 0,1")
	public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<T, T>(vertex, vertex);
		}
	}

	/**
	 * Undirected edges by emitting for each input edge the input edges itself and an inverted version.
	 */
	public static final class UndirectEdge implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
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
	public static final class NeighborWithComponentIDJoin implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}



	@ConstantFieldsFirst("0")
	public static final class ComponentIdFilter implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old, Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	public static final class UpdateSolutionSet implements CoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void coGroup(Iterable<Tuple2<Long, Long>> ss, Iterable<Tuple2<Long, Long>> delta, Collector<Tuple2<Long, Long>> c) throws Exception {
			Iterator<Tuple2<Long, Long>> it1 = ss.iterator();
			Iterator<Tuple2<Long, Long>> it2 = delta.iterator();
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

	private static String verticesPath = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		// parse input arguments
		if(programArguments.length == 4) {
			verticesPath = programArguments[0];
			edgesPath = programArguments[1];
			outputPath = programArguments[2];
			maxIterations = Integer.parseInt(programArguments[3]);
			return true;
		} else {
			System.err.println("Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
			return false;
		}
	}
}

