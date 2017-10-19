_NAME = HBaseSnapshotter-2.8
_JAR = target/scala-2.10/HBaseSnapshotter-assembly-2.8.jar

all: $(_JAR)

dist: $(_JAR)
	mkdir -p dist/$(_NAME)/lib
	cp -r bin dist/$(_NAME)
	cp -r conf dist/$(_NAME)
	cp -r $(_JAR) dist/$(_NAME)/lib
	cp LICENSE dist/$(_NAME)
	cp README.md dist/$(_NAME)
	tar -C dist -czf dist/$(_NAME).tar.gz $(_NAME)

$(_JAR):
	sbt assembly

clean:
	sbt clean

dist-clean:
	rm -rf dist
