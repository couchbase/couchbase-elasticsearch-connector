# This Makefile is used by our internal automation, its purpose is to generate
# a tarball containing all the files needed to create a container image. The
# resulting tarball is ingested in subsequent automation workflows.

product = couchbase-elasticsearch-connector
bldNum = $(if $(BLD_NUM),$(BLD_NUM),9999)
version = $(if $(VERSION),$(VERSION),1.0.0)

appDir = build/install/$(product)
artifact = $(product)-image_$(version)-$(bldNum)

dist: package-artifact

package-artifact: create-tarball
	gzip -9 dist/$(artifact).tar
	mv dist/$(artifact).{tar.gz,tgz}

create-tarball: compile
	mkdir -p dist/$(product)/$(appDir)
	cp Dockerfile dist/$(product)
	cp -a $(appDir)/* dist/$(product)/$(appDir)
	cd dist && tar -cf $(artifact).tar $(product)
	rm -rf dist/$(product)


compile: clean
	./gradlew clean installDist

clean:
	rm -rf dist

.PHONY: clean compile create-tarball package-artifact dist
