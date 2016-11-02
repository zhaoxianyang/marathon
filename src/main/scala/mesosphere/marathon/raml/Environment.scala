package mesosphere.marathon.raml

/**
  * Helpers for quick environment variable construction
  */
object Environment {
  def apply(kv: (String, String)*): Map[String, EnvVarValueOrSecret] = apply(kv.toMap)
  def apply(env: Map[String, String]): Map[String, EnvVarValueOrSecret] = env.mapValues(EnvVarValue(_))
}
