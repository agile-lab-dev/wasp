package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.ConfigManager

class ValidationRule(val key: String, val func: (ConfigManager.type) => Either[String, Unit])

object ValidationRule {
  def apply(key: String)
           (func: (ConfigManager.type) => Either[String, Unit]): ValidationRule = new ValidationRule(key, func)
}