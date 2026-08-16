description = "Deterministic whole-cluster simulator, nemesis, and invariant checks"
dependencies {
    api(project(":quorum-kv"))
    api(project(":quorum-check"))
}

// ./gradlew :quorum-sim:simSweep -Pseeds=1000 -Pstart=1
tasks.register<JavaExec>("simSweep") {
    group = "verification"
    description = "Runs the deterministic simulator across many seeds"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "io.quorum.sim.SimRunner"
    val seeds = providers.gradleProperty("seeds").orElse("200")
    val start = providers.gradleProperty("start").orElse("1")
    val extra = providers.gradleProperty("simArgs").orElse("")
    args = listOf("--seeds", seeds.get(), "--start", start.get()) +
        extra.get().split(" ").filter { it.isNotBlank() }
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(25) }
    maxHeapSize = "2g"
}

tasks.register<JavaExec>("plantedBugs") {
    group = "verification"
    description = "Asserts the simulator catches every planted bug"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = "io.quorum.sim.PlantedBugSuite"
    args = listOf("--max-seeds", providers.gradleProperty("maxSeeds").orElse("300").get())
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(25) }
    maxHeapSize = "2g"
}
