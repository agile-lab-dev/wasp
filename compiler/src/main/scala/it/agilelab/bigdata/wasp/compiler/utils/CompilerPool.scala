package it.agilelab.bigdata.wasp.compiler.utils

import io.github.andrebeat.pool.Pool

class CompilerPool(capacity: Int) extends AutoCloseable {
  override def close(): Unit = pool.close()

  val pool: Pool[Compiler] = Pool(
    capacity = this.capacity,
    factory = () => new Compiler(),
    healthCheck = _ => true, //always dispose compilers
    dispose = _.close(),
    reset = _.reset()
  )

  def use[A](action: Compiler => A): A = pool.acquire().apply(action)
}

