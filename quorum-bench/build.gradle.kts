description = "JMH microbenchmarks, db_bench-style storage bench, cluster bench and YCSB binding"
dependencies {
    implementation(project(":quorum-storage"))
    implementation(project(":quorum-server"))
    implementation(project(":quorum-client"))
    implementation(libs.jmh.core)
    implementation(libs.hdrhistogram)
    compileOnly(libs.ycsb.core)
    annotationProcessor(libs.jmh.annprocess)
}

fun benchTask(name: String, main: String, desc: String) = tasks.register<JavaExec>(name) {
    group = "benchmark"
    description = desc
    classpath = sourceSets["main"].runtimeClasspath
    mainClass = main
    args = providers.gradleProperty("benchArgs").orElse("").get().split(" ").filter { it.isNotBlank() }
    javaLauncher = javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(25) }
    maxHeapSize = "4g"
}

benchTask("jmh", "org.openjdk.jmh.Main", "Runs JMH microbenchmarks (pass -PbenchArgs='-f 1 Bloom')")
benchTask("dbBench", "io.quorum.bench.DbBench", "Single-node LSM engine benchmark")
benchTask("clusterBench", "io.quorum.bench.ClusterBench", "YCSB-style workloads against a local 3-node cluster")
