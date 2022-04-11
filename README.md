# GoTxn: a verified, concurrent, crash-safe transaction system

[![CI](https://github.com/mit-pdos/go-journal/actions/workflows/build.yml/badge.svg)](https://github.com/mit-pdos/go-journal/actions/workflows/build.yml)

GoTxn is a transaction system that makes it easy to safely access a disk with
concurrent transactions that are atomic if the system crashes in the middle. The
implementation is verified in
[Perennial](https://github.com/mit-pdos/perennial), and the proof can be found
alongside the Perennial framework.

The biggest use of GoTxn is [GoNFS](https://github.com/mit-pdos/daisy-nfsd), a
verified implementation of the Network File System (NFS) API that uses GoTxn to
simplify implementing and verifying a concurrent file system.

This repository is still called go-journal, as GoTxn evolved from a journaling
system to a transaction system. The journaling layer is still available as
`github.com/mit-pdos/go-journal/jrnl`.

## Publications

[GoJournal: a verified, concurrent, crash-safe journaling
system](https://www.chajed.io/papers/gojournal:osdi2021.pdf) at OSDI 2021
