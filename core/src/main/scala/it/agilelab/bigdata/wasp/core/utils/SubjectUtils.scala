package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.models.{SubjectStrategy, TopicModel}
import org.apache.avro.Schema

object SubjectUtils {

  def attachSubjectToSchema(prototypeTopicModel: TopicModel, avroSchema: Schema, isKey: Boolean): Schema = {
    val newSchema           = new Schema.Parser().parse(avroSchema.toString()) //cloning the schema
    val maybeSubject        = SubjectStrategy.subjectFor(avroSchema.getFullName, prototypeTopicModel, isKey)
    val maybeAlreadySubject = Option(avroSchema.getObjectProp("x-darwin-subject")).map(x => x.asInstanceOf[String])

    (maybeAlreadySubject, maybeSubject) match {
      case (Some(userSubject), Some(waspSubject)) if userSubject == waspSubject =>
      //nothing to do
      case (Some(userSubject), Some(waspSubject)) if userSubject != waspSubject =>
        throw new IllegalArgumentException(
          s"This schema already declares the subject as [${userSubject}] but wasp would like" +
            s"to set [$waspSubject], check the TopicModel Schema and the Subject Strategy"
        )
      case (Some(userSubject), None) =>
        throw new IllegalArgumentException(
          s"This schema already declares the subject as [${userSubject}] but wasp would like to not use the subject" +
            s"check the TopicModel Schema and the Subject Strategy"
        )
      case (None, Some(waspSubject)) =>
        newSchema.addProp("x-darwin-subject", waspSubject: AnyRef)
      case (None, None) =>
      // nothing to do
    }

    newSchema
  }

}
