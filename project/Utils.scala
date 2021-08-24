object Utils {
  def resolveVariable(varName: String): Option[String] = {
    sys.env.get(varName).orElse(Option(System.getProperty(varName)))
  }
  def printWithBorders(msg: String, separatorChar: String): String = {
    (separatorChar * (msg.length + 4)) + "\n" +
      (s"${separatorChar} ${msg} ${separatorChar}\n") +
      (separatorChar * (msg.length + 4))
  }
}