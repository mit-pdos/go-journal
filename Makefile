GOPATH		:= $(shell go env GOPATH)
GOOSE_DIRS	:= buf util common addr wal alloc jrnl obj lockmap jrnl_replication txn

COQ_PKGDIR := Goose/github_com/mit_pdos/go_journal

all: check goose-output

check:
	test -z $$(gofmt -d .)
	go vet ./...

goose-output: $(patsubst %,${COQ_PKGDIR}/%.v,$(GOOSE_DIRS))

${COQ_PKGDIR}/%.v: % %/*
	$(GOPATH)/bin/goose -out Goose ./$<

clean:
	rm -rf Goose
