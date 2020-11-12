GOPATH		:= $(shell go env GOPATH)
GOOSE_DIRS	:= buf util common addr wal alloc super buftxn cache fh fstxn txn inode lockmap kvs nfstypes simple buftxn_replication dir

# Things that don't goose yet:
#   .
#   dcache
#   nfstypes: need to ignore nfs_xdr.go

COQ_PKGDIR := Goose/github_com/mit_pdos/goose_nfsd

all: check goose-output

check:
	test -z $$(gofmt -d .)
	go vet ./...

goose-output: $(patsubst %,${COQ_PKGDIR}/%.v,$(GOOSE_DIRS))

${COQ_PKGDIR}/%.v: % %/*
	$(GOPATH)/bin/goose -package github.com/mit-pdos/goose-nfsd/$< -out Goose ./$<

${COQ_PKGDIR}/nfstypes.v: nfstypes/nfs_types.go
	$(GOPATH)/bin/goose -package github.com/mit-pdos/goose-nfsd/$< -out Goose ./nfstypes/goose-workaround/nfstypes

clean:
	rm -rf Goose
