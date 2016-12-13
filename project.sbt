name := "HBaseSnapshotter"

version := "2.0"

scalaVersion := "2.11.1"

exportJars := true

resolvers += Resolver.sonatypeRepo("public")

updateOptions := updateOptions.value.withLatestSnapshots(false)
