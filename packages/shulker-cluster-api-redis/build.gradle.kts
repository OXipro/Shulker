configure<JavaPluginExtension> {
    withJavadocJar()
    withSourcesJar()
}

dependencies {
    // PlayerPosition/RegisteredProxy/RegisteredServer/MessagingBus
    api(project(":packages:shulker-cluster-api"))

    // JedisPool/Jedis types are part of this module's public surface
    // (RedisCacheAdapter, RedisPubSubAdapter, RedisConfiguration).
    api(libs.jedis)

    // shulker-cluster-api only declares adventure as `compileOnlyApi` (it
    // assumes a host platform provides it). We actually use it at runtime
    // here (RedisPubSubAdapter kick messages), so redeclare for real -
    // adventure-text-serializer-gson pulls in JSONComponentSerializer and
    // gson transitively, both used by adapters in this module.
    implementation(libs.adventure.api)
    implementation(libs.adventure.text.serializer.gson)
}
