configure<JavaPluginExtension> {
    withJavadocJar()
    withSourcesJar()
}

dependencies {
    api(project(":packages:shulker-cluster-api"))

    // RedisCacheAdapter/RedisPubSubAdapter/RedisConfiguration - none of it
    // depends on Kubernetes or Agones, unlike shulker-cluster-api-impl.
    // This is the whole point of this module: no in-cluster deps at all.
    api(project(":packages:shulker-cluster-api-redis"))
}
