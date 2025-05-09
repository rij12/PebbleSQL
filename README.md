# PebbleSQL-
SQL Database written in Golang

## Core Objectives
* Slotted Pages [x]
* Disk Manager for DB File [x]
* Fully Implement B-Link Tree []
* Write ahead log (Crash recovery) []
* Service layer for ACID [] 
* SQL parsing []
* SQL optimisation []

## Gotta go fast
* MMAP Files? []
* Kernal Bypass? []
* Direct I/O? []

## Networking 
* Distrubted Raft? Paxos? [] 
* Networking kernal bypass? []

## Things missing from MVP 

* Currently can only support one index, as value as in lined in the slotted pages instead of using RowID's 
* Pages need to be de-fragmented

## Learning & References
* Database Internals by Alex Petrov 
