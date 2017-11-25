package net.wrap_trap.calcite_arrow_sample

import java.io.File

import org.apache.calcite.model.ModelHandler
import org.apache.calcite.schema.{Schema, SchemaFactory, SchemaPlus}
import org.slf4j.LoggerFactory

/**
  * Created by masayuki on 2017/11/18.
  */
class ArrowSchemaFactory extends SchemaFactory {
  val logger = LoggerFactory.getLogger(classOf[ArrowSchemaFactory])

  override def create(parentSchema: SchemaPlus, name: String, operand: java.util.Map[String, Object]): Schema = {
    logger.error("create")
    val directory = operand.get("directory").asInstanceOf[String]
    val base = operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName).asInstanceOf[File]
    var directoryFile = new File(directory)
    if (base != null && !directoryFile.isAbsolute) {
      directoryFile = new File(base, directory)
    }
    new ArrowSchema(directoryFile)
  }
}
