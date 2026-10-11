.PHONY: docs-generate test-apiserver test-etcd-diff test-robustness

docs-generate:
	go run ./hack/docgen

test-apiserver:
	tests/apiserver/run.sh

test-etcd-diff:
	cd tests/etcddiff && go test -count=1 -timeout 10m ./...

test-robustness:
	cd tests/robustness && go test -count=1 -timeout 30m ./...
