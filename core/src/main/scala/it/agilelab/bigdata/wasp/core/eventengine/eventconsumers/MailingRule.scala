package it.agilelab.bigdata.wasp.core.eventengine.eventconsumers


/**
  * Mailing rules store information about how to craft and send an e-mail. Mailing rules store an SQL statement which
  * indicates whether or not an e-mail should be sent for the specific Event, and another statement to enrich it with mail subject.
  * In the mailing rules are also specified mail recipient info to use, and the path to the velocity template to use to compose the e-mail.
  *
  * @param mailingRuleName is the name of the MailingRule
  * @param ruleStatement defines whether a Mail should be sent or not
  * @param subjectStatement enrich the mail with a subject. WARNING: this statement is ignored if the mail aggregation options is enabled. FIXME: avoid
  * @param templatePath is the path to the velocity template to use to compose this e-mail. It has to be available in the driver.
  * @param mailTo is the main recipient of the e-mail. Can be a single address or a list of comma separated addresses
  * @param mailCc is the optional CC recipient of the e-mail. Can be a single address or a list of comma separated addresses
  * @param mailBcc is the optional BCC recipient of the e-mail. Can be a single address or a list of comma separated addresses
  */
case class MailingRule(
                        mailingRuleName: String,
                        ruleStatement: String, //sql expression
                        subjectStatement: String,
                        templatePath: String,
                        mailTo: String,
                        mailCc: Option[String],
                        mailBcc: Option[String])
