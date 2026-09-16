plugins {
    application
}
description = "Java client library and CLI"
dependencies {
    api(project(":quorum-kv"))
    implementation(project(":quorum-net"))
}
application {
    mainClass = "io.quorum.client.Cli"
    applicationName = "quorum"
}
