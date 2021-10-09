package it.agilelab.bigdata.utils

object ClasspathUtils {
  def locations(clz: Class[_], classLoader:ClassLoader): List[String] = {
    val enums = classLoader.getResources(clz.getCanonicalName.replace(".","/"))
    val list = List.newBuilder[String]
    while (enums.hasMoreElements){
      list. += (enums.nextElement().toString)
    }
    list.result()
  }
  def locations(clz: Class[_]): List[String] = {
    locations(clz, getClass.getClassLoader)
  }
}
