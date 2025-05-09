# PebbleSQL -
SQL Database written in Golang

## Core Objectives

Goal: Build a minimal working SQL database with basic storage and query support.

- [x] Slotted Pages
- [x] Disk Manager for DB File
- [ ] Fully Implement B-Link Tree
- [ ] Write ahead log (Crash recovery)
- [ ] Service layer for ACID
- [ ] SQL parsing
- [ ] SQL optimisation

## Gotta go fast
- [ ] MMAP Files?
- [ ] Kernel Bypass?
- [ ] Direct I/O?

## Networking
- [ ] Distributed Raft? Paxos?
- [ ] Networking kernel bypass?

## Things missing from MVP
- Currently can only support one index, as value is inlined in the slotted pages instead of using RowIDs
- Pages need to be de-fragmented

## Learning & References
- *Database Internals* by Alex Petrov  
