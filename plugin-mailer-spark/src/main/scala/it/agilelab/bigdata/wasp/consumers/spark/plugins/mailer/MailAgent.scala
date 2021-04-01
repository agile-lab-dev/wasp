package it.agilelab.bigdata.wasp.consumers.spark.plugins.mailer

import java.util.Properties

import javax.mail._
import javax.mail.internet.{InternetAddress, MimeMessage}

trait MailAgent {
  def send(mail: Mail): Unit
}

case class ConsoleMailAgent() extends MailAgent {
  override def send(mail: Mail): Unit = println(s"INFO - ${System.currentTimeMillis()} ConsoleMailAgent: "  + mail.toString)
}

case class MailAgentImpl(options: Map[String, String]) extends MailAgent {

  private val sanitizedOptions = options.map( e => (e._1.replace("___", "."), e._2) )

  // Mandatory fields
  private val mailFrom = sanitizedOptions("mail-from")
  private val host = sanitizedOptions("mail.smtp.host")
  private val port: Integer = sanitizedOptions("mail.smtp.port").toInt //Integer because has to be inserted into a java properties object
  private val username = sanitizedOptions("username")
  private val password = sanitizedOptions("password")

  //Optional/Extra fields
  private val extraOptions = sanitizedOptions.filterNot(e => {
      Seq(
        "mail-from",
        "mail.smtp.host",
        "mail.smtp.port",
        "username",
        "password").contains(e._1)
    })

  //Java-ish way to get a session instance
  private lazy val session: Session = {

    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.host", host)
    properties.put("mail.transport.protocol", "smtp")
//    properties.putAll(extraOptions.asJava)
    extraOptions.map(v => properties.put(v._1, v._2))

    val authenticator: Authenticator = new Authenticator {
      override def getPasswordAuthentication: PasswordAuthentication = new PasswordAuthentication(username, password)}

    //TODO: create a pool of sessions because a single session is synchronized
    Session.getInstance(properties, authenticator)
  }

  def send(mail: Mail): Unit = {

    val msg = new MimeMessage(session)

    msg.setFrom(new InternetAddress(mailFrom))

    setMessageRecipients(msg, mail.mailTo, Message.RecipientType.TO)
    if (mail.mailCc.isDefined) {
      setMessageRecipients(msg, mail.mailCc.get, Message.RecipientType.CC)
    }
    if (mail.mailBcc.isDefined) {
      setMessageRecipients(msg, mail.mailBcc.get, Message.RecipientType.BCC)
    }

    msg.setSentDate(mail.mailSendDate)
    msg.setSubject(mail.mailSubject)
    msg.setText(mail.mailContent)

    println(s"READY TO SEND MAIL: ${mail.toString.replaceAll("\n", "\\n")}")

    Transport.send(msg)
  }

  // throws AddressException, MessagingException
  private def setMessageRecipients(msg: Message, recipient: String, recipientType: Message.RecipientType): Unit = {
    // had to do the asInstanceOf[...] call here to make scala happy
    val addressArray = buildInternetAddressArray(recipient).asInstanceOf[Array[Address]]
    if ((addressArray != null) && (addressArray.length > 0))
    {
      msg.setRecipients(recipientType, addressArray)
    }
  }

  // throws AddressException
  private def buildInternetAddressArray(address: String): Array[InternetAddress] = {
    // could test for a null or blank String but I'm letting parse just throw an exception
    InternetAddress.parse(address)
  }

}