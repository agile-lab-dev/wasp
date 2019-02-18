package it.agilelab.bigdata.wasp.consumers.spark.plugins.mailer

import java.util.Date

case class Mail (
  mailTo: String,
  mailCc: Option[String],
  mailBcc: Option[String],
  mailSubject: String,
  mailContent: String,
  contentType: String = "text/html"
){

  val mailSendDate = new Date

  override def toString: String = {
    val lBuilder = new StringBuilder
    //lBuilder.append("Mail From:- ").append(mailFrom)
    lBuilder.append("Mail To:- ").append(mailTo)
    lBuilder.append("Mail Cc:- ").append(mailCc.getOrElse(""))
    lBuilder.append("Mail Bcc:- ").append(mailBcc.getOrElse(""))
    lBuilder.append("Mail Subject:- ").append(mailSubject)
    lBuilder.append("Mail Send Date:- ").append(mailSendDate)
    lBuilder.append("Mail Content:- ").append(mailContent)
    lBuilder.toString()
  }
}