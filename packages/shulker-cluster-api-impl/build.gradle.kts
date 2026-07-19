dependencies {
    api(project(":packages:shulker-cluster-api"))
    api(project(":packages:shulker-cluster-api-redis"))

    // Agones
    api(project(":packages:google-agones-sdk"))

    // Kubernetes
    compileOnlyApi(libs.kubernetes.client.api)
    runtimeOnly(libs.kubernetes.client)
    implementation(libs.kubernetes.client.http)
}
