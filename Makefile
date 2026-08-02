SHELL := /bin/sh

FUZZ_TIME ?= 1s
BENCH_TIME ?= 100ms
RACE_COUNT ?= 3

.PHONY: benchmark check docs fmt fuzz race test vet

check:
	$(MAKE) -C ../.. check MODULES=pkg/adaptive-throttle

fmt:
	@files="$$(find . -type f -name '*.go')"; \
		test -z "$$(gofmt -l $$files)" || { gofmt -l $$files; exit 1; }

vet:
	go vet ./...

test:
	go test ./... -count=1

race:
	go test -race -count=$(RACE_COUNT) ./...

fuzz:
	go test -run='^$$' -fuzz=FuzzGoogleSREProbabilityFiniteAndBounded -fuzztime=$(FUZZ_TIME) .
	go test -run='^$$' -fuzz=FuzzBucketIndexBounded -fuzztime=$(FUZZ_TIME) .
	go test -run='^$$' -fuzz=FuzzBoundedEventSequences -fuzztime=$(FUZZ_TIME) .

docs:
	go test -run='^Example' ./...
	go list -f '{{if .GoFiles}}{{.ImportPath}}{{end}}' ./... | xargs -n 1 go doc >/dev/null

benchmark:
	go test -run='^$$' -bench=. -benchmem -benchtime=$(BENCH_TIME) ./...
