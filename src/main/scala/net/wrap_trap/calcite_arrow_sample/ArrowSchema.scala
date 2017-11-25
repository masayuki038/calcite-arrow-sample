package net.wrap_trap.calcite_arrow_sample

import java.io.File
import java.nio.file.{FileSystems, Files}
import java.util

import net.wrap_trap.calcite_arrow_sample.Helper._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.file.{ArrowFileReader, SeekableReadChannel}
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.AbstractSchema
import org.slf4j.LoggerFactory

/**
  * Created by masayuki on 2017/11/18.
  */
class ArrowSchema(val directory: File) extends AbstractSchema {
  val logger = LoggerFactory.getLogger(classOf[ArrowSchema])

  private def trim(s: String, suffix: String): String = {
    val trimmed = trimOrNull(s, suffix)
    trimmed match {
      case null => s
      case _ => trimmed
    }
  }

  private def trimOrNull(s: String, suffix: String): String = {
    s.endsWith(suffix) match {
      case true => s.substring(0, s.length() - suffix.length)
      case false => null
    }
  }

  override def getTableMap: util.Map[String, Table] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val map = new util.HashMap[String, Table]
    logger.warn(directory.getAbsolutePath)
    directory.listFiles((dir: File, name: String) => name.endsWith(".arrow"))
      .foreach { f =>
        map.put(
          trim(f.getName(), ".arrow").toUpperCase,
          new ArrowScannableTable(load(f.getAbsolutePath, allocator), null))
      }
    map
  }

  def load(path: String, allocator: BufferAllocator): VectorSchemaRoot = {
    val byteArray = Files.readAllBytes(FileSystems.getDefault().getPath(path))
    val channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(byteArray))
    using(new ArrowFileReader(channel, allocator)) { reader =>
      reader.getRecordBlocks().forEach(block => reader.loadRecordBatch(block))
      reader.getVectorSchemaRoot
    }
  }
}
