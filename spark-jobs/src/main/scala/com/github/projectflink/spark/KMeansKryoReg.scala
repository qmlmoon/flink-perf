package com.github.projectflink.spark

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.github.projectflink.spark.KMeansArbitraryDimension.Point


class KMeansKryoReg extends KryoRegistrator {

	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[Point])
	}

}
