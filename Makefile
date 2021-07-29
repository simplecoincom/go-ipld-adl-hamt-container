.PHONY: build clean deploy

rebuild: clean build

test: build
	# Don't cache tests --count=1
	go test -v --count=1 ./...