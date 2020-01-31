GOPATH		:= $(shell go env GOPATH)
GOOSE_DIRS	:= buf util wal alloc bcache fs buftxn cache fh fstxn txn

# Things that don't goose yet:
#   dcache
#   dir
#   inode
#   txn: need support for sorting
#   nfstypes: need to ignore nfs_xdr.go

all:	$(patsubst %,goose/%.v,$(GOOSE_DIRS))

goose/%.v: % %/*
	@mkdir -p $(@D)
	$(GOPATH)/bin/goose -out $@ $<

clean:
	rm -f goose/*.v
