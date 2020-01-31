GOPATH		:= $(shell go env GOPATH)
GOOSE_DIRS	:= buf util wal

all:	$(patsubst %,goose/%.v,$(GOOSE_DIRS))

goose/%.v: % %/*
	@mkdir -p $(@D)
	$(GOPATH)/bin/goose -out $@ $<

clean:
	rm -f goose/*.v
