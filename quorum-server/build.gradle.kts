plugins {
    application
}
description = "Quorum server process"
dependencies {
    api(project(":quorum-net"))
    testImplementation(project(":quorum-client"))
    testImplementation(project(":quorum-check"))
}
application {
    mainClass = "io.quorum.server.QuorumServer"
    applicationName = "quorum-server"
}
