# GoJournal: a verified, concurrent, crash-safe journaling system

[![CI](https://github.com/mit-pdos/go-journal/actions/workflows/build.yml/badge.svg)](https://github.com/mit-pdos/go-journal/actions/workflows/build.yml)

GoJournal is a journaling system that makes disk operations atomic. It supports
concurrent operations with good performance. The implementation is verified in
[Perennial](https://github.com/mit-pdos/perennial), and the proof can be found
alongside the Perennial framework.

The biggest application of GoJournal is
[GoNFS](https://github.com/mit-pdos/go-nfsd/), an unverified NFS server that
gets good performance thanks to the journaling system.

## Publications

[GoJournal: a verified, concurrent, crash-safe journaling
system](https://www.chajed.io/papers/gojournal:osdi2021.pdf) at OSDI 2021
