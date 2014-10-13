package com.github.projectflink.spark

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.github.projectflink.spark.TPCHQuery3.{ShipPriorityItem, Customer, Order, Lineitem}


class TPCH3KryoReg extends KryoRegistrator {

	override def registerClasses(kryo: Kryo) {
		kryo.register(classOf[Lineitem])
		kryo.register(classOf[Order])
		kryo.register(classOf[Customer])
		kryo.register(classOf[ShipPriorityItem])
	}

}
