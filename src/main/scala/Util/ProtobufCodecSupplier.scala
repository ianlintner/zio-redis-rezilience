package Util

import zio.{ULayer, ZLayer}
import zio.redis.CodecSupplier
import zio.schema.Schema
import zio.schema.codec.{BinaryCodec, ProtobufCodec}

object ProtobufCodecSupplier extends CodecSupplier {
  def get[A: Schema]: BinaryCodec[A] = ProtobufCodec.protobufCodec

  def layer: ULayer[CodecSupplier] = ZLayer.succeed[CodecSupplier](ProtobufCodecSupplier)
}
