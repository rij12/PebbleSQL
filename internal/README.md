# Internal Components 

## General Flow diagram 

```
┌──────────────────────────────┐
│       SQL / Query Layer      │ ← Parser, planner, optimizer
├──────────────────────────────┤
│       Table & Index Layer    │ ← Logical records and indexes
├──────────────────────────────┤
│       B-Tree / LSM Layer     │ ← Index & heap organization
├──────────────────────────────┤
│        Slotted Page Layer    │ ← Layout of keys/data on a page
├──────────────────────────────┤
│        Buffer/Cache Manager  │ ← In-memory page access + pin/unpin
├──────────────────────────────┤
│         Disk Manager         │ ← Read/write raw pages to disk
└──────────────────────────────┘
```