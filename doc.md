# Building a Database Engine in Go — Architectural Design

## Full Layer Stack

```mermaid
graph TB
    subgraph "SQL Layer"
        SQL["SQL Parser → AST → Planner → Executor"]
    end

    subgraph "Table & Index Layer"
        TBL["Schema Catalog / Table Heap / Index Scan"]
    end

    subgraph "B-Tree & LSM Layer"
        IDX["B+Tree (indexes) / LSM Tree (optional write-optimised path)"]
    end

    subgraph "Slotted Page Layer"
        SP["Page Layout / Slot Directory / Tuple Read-Write"]
    end

    subgraph "Buffer & Cache Manager"
        BUF["Page Buffer Pool / Pin-Unpin / Eviction (LRU-Clock)"]
    end

    subgraph "Disk Manager"
        DISK["File I/O / Page Alloc-Free / Free List"]
    end

    SQL --> TBL
    TBL --> IDX
    TBL --> SP
    IDX --> SP
    SP --> BUF
    BUF --> DISK

    style SQL fill:#4a6fa5,color:#fff
    style TBL fill:#5b8c5a,color:#fff
    style IDX fill:#8b6f47,color:#fff
    style SP fill:#7a5195,color:#fff
    style BUF fill:#c2555a,color:#fff
    style DISK fill:#555,color:#fff
```

> **Key rule:** every layer only talks to the layer directly below it. The SQL layer never touches disk. The Disk Manager knows nothing about tuples.

---

## 1 — Disk Manager

The foundation. It owns the physical database file and thinks purely in fixed-size **pages** (4 KB is a good default). Nothing above this layer ever calls `os.Read` or `os.Write` directly.

### Responsibilities

| Concern | Detail |
|---|---|
| **Page size** | Constant (e.g. 4096 bytes). Every read/write is exactly one page. |
| **Page addressing** | A `PageID` is just a `uint32` offset: byte offset = `PageID × PageSize`. |
| **Allocate page** | Pop from the free list, or grow the file by one page. |
| **Free page** | Push `PageID` back onto the free list. |
| **Read / Write** | `ReadPage(id) → []byte` and `WritePage(id, data)`. |
| **Free list** | A linked list of freed pages stored *inside* the file itself (page 0 is the header). |

### File Layout

```mermaid
block-beta
    columns 6
    P0["Page 0\nFile Header\n• page count\n• free list head\n• magic bytes"]:1
    P1["Page 1\nData / Index\nPage"]:1
    P2["Page 2\nData / Index\nPage"]:1
    P3["Page 3\n(freed)\nnext_free → 5"]:1
    P4["Page 4\nData / Index\nPage"]:1
    P5["Page 5\n(freed)\nnext_free → 0 (nil)"]:1

    style P0 fill:#555,color:#fff
    style P3 fill:#888,color:#fff
    style P5 fill:#888,color:#fff
```

### Core Interface to Design

```mermaid
classDiagram
    class DiskManager {
        -file *os.File
        -pageSize uint32
        -pageCount uint32
        -freeListHead PageID
        +NewDiskManager(path string) DiskManager
        +AllocatePage() PageID
        +FreePage(id PageID)
        +ReadPage(id PageID) []byte
        +WritePage(id PageID, data []byte)
        +Close()
    }
```

### Design Decisions to Make Early

1. **Single file vs multi-file?** Start with a single file. Simpler seeking logic.
2. **Page 0 header format** — define a struct for it: magic number, version, page size, page count, free list head. Serialize it with `encoding/binary`.
3. **Concurrency** — for now, a single `sync.Mutex` on the whole manager is fine. Real databases use more granular locking.
4. **`pread`/`pwrite` vs `Seek+Read`** — Go's `File.ReadAt` and `File.WriteAt` are offset-based and safe for concurrent use on the same `*os.File`, so prefer those.

---

## 2 — Buffer & Cache Manager

The database never reads from disk into the layer that needs it directly. Instead, every page lives in a **buffer pool** — a fixed-size array of in-memory page frames. This is *your* page cache (you don't rely on the OS page cache).

### Core Concepts

```mermaid
graph LR
    subgraph "Buffer Pool (e.g. 256 frames)"
        F0["Frame 0\nPageID: 7\npinned: 2\ndirty: yes"]
        F1["Frame 1\nPageID: 3\npinned: 0\ndirty: no"]
        F2["Frame 2\n(empty)"]
        F3["Frame 3\nPageID: 12\npinned: 1\ndirty: yes"]
    end

    subgraph "Page Table (map)"
        PT["PageID → FrameIndex\n7 → 0\n3 → 1\n12 → 3"]
    end

    PT --> F0
    PT --> F1
    PT --> F3
```

### Pin / Unpin Lifecycle

```mermaid
sequenceDiagram
    participant Caller as Upper Layer
    participant BM as BufferManager
    participant Pool as Frame Pool
    participant DM as DiskManager

    Caller->>BM: FetchPage(pageID=7)
    BM->>BM: Check page table for pageID 7

    alt Page found in pool
        BM->>Pool: Increment pin count on frame
        BM-->>Caller: Return *Page (pinned)
    else Page NOT in pool
        BM->>BM: Find victim frame (LRU / Clock)
        alt Victim is dirty
            BM->>DM: WritePage(victim.pageID, victim.data)
        end
        BM->>DM: ReadPage(7)
        DM-->>BM: raw bytes
        BM->>Pool: Load into victim frame, set pageID=7, pin=1
        BM-->>Caller: Return *Page (pinned)
    end

    Note over Caller: Use the page...

    Caller->>BM: UnpinPage(pageID=7, dirty=true)
    BM->>Pool: Decrement pin count, mark dirty
```

### Eviction — Clock Algorithm

The Clock algorithm is simpler than full LRU and works well. Think of frames arranged in a circle with a "clock hand":

```mermaid
graph TB
    subgraph "Clock Sweep"
        direction LR
        A["Frame 0\nref=1"] --> B["Frame 1\nref=0\n✓ VICTIM"]
        B --> C["Frame 2\nref=1"]
        C --> D["Frame 3\nref=1"]
        D --> A
    end

    Hand["Clock Hand →"] --> B

    style B fill:#c2555a,color:#fff
```

**Rules:** When you need a victim: advance the hand. If `ref bit == 1`, set it to `0` and move on. If `ref bit == 0` and `pin count == 0`, evict that frame. On every `FetchPage`, set the frame's ref bit to `1`.

### Core Interface

```mermaid
classDiagram
    class BufferManager {
        -pool []Frame
        -pageTable map[PageID]FrameIndex
        -diskManager *DiskManager
        -clockHand int
        +NewBufferManager(size int, dm *DiskManager) BufferManager
        +FetchPage(id PageID) *Page
        +UnpinPage(id PageID, dirty bool)
        +FlushPage(id PageID)
        +FlushAll()
    }

    class Frame {
        +pageID PageID
        +data [PAGE_SIZE]byte
        +pinCount int
        +dirty bool
        +refBit bool
    }

    BufferManager "1" *-- "N" Frame
```

### Key Design Decisions

1. **Pool size** — make it configurable. 256 frames × 4 KB = 1 MB. Tiny but perfect for learning.
2. **`*Page` wrapper** — the pointer you return to callers should give access to the raw `[]byte` data *and* the `PageID`, but should not let callers mess with pin counts directly.
3. **When to flush** — only on eviction or explicit `FlushPage`/`FlushAll`. This gives you write batching for free.
4. **Thread safety** — one `sync.RWMutex` on the whole pool to start with.

---

## 3 — Slotted Page Layer

This layer gives **structure** to the raw 4 KB byte slabs. It implements a slotted page layout so you can store variable-length tuples (rows) and address them by `(PageID, SlotIndex)`.

### Page Internal Layout

```mermaid
block-beta
    columns 1
    block:header["Page Header (fixed, e.g. 16 bytes)"]
        columns 4
        A["page_id\n4B"]
        B["slot_count\n2B"]
        C["free_space\noffset 2B"]
        D["flags /\nreserved 8B"]
    end
    block:slots["Slot Directory (grows →)"]
        columns 4
        S0["Slot 0\noffset + len"]
        S1["Slot 1\noffset + len"]
        S2["Slot 2\n(deleted)"]
        S3["Slot 3\noffset + len"]
    end
    FREE["Free Space\n(shrinks from both ends as page fills)"]
    block:tuples["Tuple Data (grows ←)"]
        columns 3
        T3["Tuple 3 bytes"]
        T1["Tuple 1 bytes"]
        T0["Tuple 0 bytes"]
    end

    style header fill:#555,color:#fff
    style slots fill:#7a5195,color:#fff
    style FREE fill:#ddd,color:#333
    style tuples fill:#4a6fa5,color:#fff
```

**The slot directory grows forward from the header. Tuple data grows backward from the end of the page. They meet in the middle when the page is full.**

### Tuple Addressing

```mermaid
graph LR
    TID["TupleID\n(PageID=4, Slot=1)"] --> Page4["Page 4"]
    Page4 --> SlotDir["Slot Directory\nSlot 1 → offset: 4050, len: 38"]
    SlotDir --> Bytes["Bytes 4050..4087\n= raw tuple"]

    style TID fill:#5b8c5a,color:#fff
```

A **TupleID** (sometimes called RID — Record ID) is always `(PageID, SlotIndex)`. This is the physical address every upper layer uses to refer to a row.

### Insert / Delete / Update Flow

```mermaid
flowchart TD
    INS["InsertTuple(pageID, data)"]
    INS --> CHECK{"Enough free space\non this page?"}
    CHECK -->|Yes| WRITE["1. Write tuple bytes at free_space_offset\n2. Add slot entry (offset, len)\n3. Update free_space_offset\n4. Increment slot_count"]
    CHECK -->|No| FAIL["Return error\n(caller must allocate new page)"]

    DEL["DeleteTuple(pageID, slotIndex)"]
    DEL --> MARK["Mark slot as deleted\n(set offset to a sentinel like 0)\nDo NOT shift other tuples"]
    MARK --> COMPACT{"Optional:\nCompact page?"}
    COMPACT -->|Lazy| SKIP["Leave gap, reclaim on next insert\nor explicit compaction"]
    COMPACT -->|Eager| DEFRAG["Rewrite all tuples contiguously\nand update slot offsets"]

    UPD["UpdateTuple(pageID, slotIndex, newData)"]
    UPD --> SIZE{"Same size\nor smaller?"}
    SIZE -->|Yes| INPLACE["Overwrite in place"]
    SIZE -->|No, bigger| REINSERT["Delete + Insert\n(may move to new page → forward pointer)"]
```

### Core Interface

```mermaid
classDiagram
    class SlottedPage {
        -raw []byte
        +PageID() PageID
        +SlotCount() uint16
        +FreeSpace() uint16
        +InsertTuple(data []byte) (SlotIndex, error)
        +DeleteTuple(slot SlotIndex) error
        +GetTuple(slot SlotIndex) ([]byte, error)
        +UpdateTuple(slot SlotIndex, data []byte) error
    }

    class TupleID {
        +PageID PageID
        +Slot SlotIndex
    }
```

### Design Decisions

1. **Slot entry size** — 4 bytes is enough: 2 bytes for offset within the page (max 65535, more than your 4096 page), 2 bytes for length.
2. **Deletion strategy** — use a tombstone in the slot directory. Don't physically remove the tuple immediately. This keeps all existing `TupleID`s stable.
3. **Page compaction** — implement it, but call it lazily (only when an insert fails due to fragmentation but total free bytes are sufficient).
4. **This layer does NOT know about the buffer pool.** It receives a `[]byte` slice and operates purely on the bytes. The caller (Table layer) fetches/unpins pages via the Buffer Manager.

---

## 4 — B-Tree & LSM Layer

This is where you get fast lookups. A **B+Tree** is the standard choice for a disk-oriented database. An **LSM Tree** is optional but great for learning about write-optimised storage.

### B+Tree Structure

```mermaid
graph TD
    ROOT["Internal Node (Root)\nKeys: [20, 50]\nChildren: [P1, P2, P3]"]
    ROOT -->|"< 20"| L1["Leaf Node P1\n(5,TID) (12,TID) (18,TID)\nnext → P2"]
    ROOT -->|"20..49"| L2["Leaf Node P2\n(20,TID) (35,TID) (42,TID)\nnext → P3"]
    ROOT -->|"≥ 50"| L3["Leaf Node P3\n(50,TID) (67,TID) (88,TID)\nnext → nil"]

    L1 -->|"next leaf"| L2
    L2 -->|"next leaf"| L3

    style ROOT fill:#8b6f47,color:#fff
    style L1 fill:#5b8c5a,color:#fff
    style L2 fill:#5b8c5a,color:#fff
    style L3 fill:#5b8c5a,color:#fff
```

**Key properties of a B+Tree:**
- All actual data pointers (TupleIDs) live in **leaf nodes** only.
- Internal nodes store only keys + child page pointers (used for routing).
- Leaf nodes are linked together (next-leaf pointer) so range scans just follow the chain.
- Each node is exactly **one page** on disk.

### B+Tree Search

```mermaid
flowchart TD
    START["Search(key=35)"] --> ROOT["Read root page\nKeys: [20, 50]"]
    ROOT --> CMP{"35 < 20?"}
    CMP -->|No| CMP2{"35 < 50?"}
    CMP2 -->|Yes| CHILD["Follow middle child → Page P2"]
    CHILD --> LEAF["Leaf P2: scan entries\n(20,TID) (35,TID✓) (42,TID)"]
    LEAF --> FOUND["Return TupleID for key 35"]

    style FOUND fill:#5b8c5a,color:#fff
```

### B+Tree Insert & Split

```mermaid
flowchart TD
    INS["Insert(key=25, tupleID)"]
    INS --> FIND["Search tree to find target leaf"]
    FIND --> FIT{"Leaf has\nroom?"}
    FIT -->|Yes| ADD["Insert (key, TID) into leaf\nin sorted position"]
    FIT -->|No| SPLIT["Split leaf into two:\nLeft: first ⌈n/2⌉ entries\nRight: remaining entries"]
    SPLIT --> PUSH["Push middle key UP\ninto parent internal node"]
    PUSH --> PFIT{"Parent has\nroom?"}
    PFIT -->|Yes| DONE["Done"]
    PFIT -->|No| PSPLIT["Split parent too\n(recursive — can propagate to root)"]
    PSPLIT --> NEWROOT{"Root split?"}
    NEWROOT -->|Yes| GROW["Create new root\nTree height increases by 1"]
    NEWROOT -->|No| DONE
```

### Node Layout (reuse your Slotted Page!)

```mermaid
block-beta
    columns 1
    block:hdr["Node Header"]
        columns 5
        A["is_leaf\n1B"]
        B["key_count\n2B"]
        C["parent\nPageID 4B"]
        D["next_leaf\nPageID 4B"]
        E["reserved"]
    end
    block:entries["Key-Pointer Pairs"]
        columns 1
        F["For internal: [childPtr₀] [key₁, childPtr₁] [key₂, childPtr₂] ..."]
        G["For leaf: [key₁, TupleID₁] [key₂, TupleID₂] ..."]
    end

    style hdr fill:#8b6f47,color:#fff
    style entries fill:#555,color:#fff
```

### Optional: LSM Tree

If you want to explore write-optimised storage after the B+Tree works:

```mermaid
flowchart LR
    subgraph "Memory"
        MT["Memtable\n(sorted, e.g. Go map\nor skip list)"]
        WAL["Write-Ahead Log\n(append-only file)"]
    end

    subgraph "Disk — Level 0"
        SST0a["SSTable 0a"]
        SST0b["SSTable 0b"]
    end

    subgraph "Disk — Level 1 (merged, larger)"
        SST1["SSTable 1\n(sorted, merged)"]
    end

    Write["Write(key, val)"] --> WAL
    Write --> MT
    MT -->|"Flush when full"| SST0a
    SST0a --> COMPACT["Compaction\n(merge sort)"]
    SST0b --> COMPACT
    COMPACT --> SST1

    Read["Read(key)"] --> MT
    MT -->|miss| SST0a
    SST0a -->|miss| SST0b
    SST0b -->|miss| SST1
```

### Design Decisions

1. **Start with B+Tree.** It's the core of every traditional RDBMS and teaches you the most about page-oriented index structures.
2. **Order / fan-out** — calculate how many keys fit in one 4 KB page given your key size (e.g. 8-byte int keys + 4-byte child pointers → ~200 keys per node).
3. **B+Tree node = one page.** Use the Buffer Manager to fetch/unpin nodes. Each node's PageID *is* its address.
4. **Deletion** — start with lazy deletion (mark key as deleted). Full B+Tree merge/rebalance is complex and can be a later enhancement.
5. **LSM** — treat as a stretch goal. Implement B+Tree first.

---

## 5 — Table & Index Layer

This layer introduces the **logical** concepts of tables, rows, columns, schemas, and indexes. Below here everything was physical pages and bytes; from here up it's relational.

### Catalog / Schema Design

```mermaid
classDiagram
    class Catalog {
        -tables map[string]*TableMeta
        +CreateTable(name string, schema Schema) error
        +DropTable(name string) error
        +GetTable(name string) *TableMeta
        +ListTables() []string
    }

    class TableMeta {
        +Name string
        +Schema Schema
        +HeapFileStartPage PageID
        +Indexes []IndexMeta
    }

    class Schema {
        +Columns []Column
        +PrimaryKey string
        +Encode(row Row) []byte
        +Decode(data []byte) Row
    }

    class Column {
        +Name string
        +Type ColumnType
        +Nullable bool
        +MaxLength int
    }

    class IndexMeta {
        +Name string
        +ColumnName string
        +RootPage PageID
        +IsUnique bool
    }

    Catalog "1" *-- "*" TableMeta
    TableMeta "1" *-- "1" Schema
    Schema "1" *-- "*" Column
    TableMeta "1" *-- "*" IndexMeta
```

### Table Heap — Where Rows Live

A table's data is a linked list of slotted pages (a "heap file"):

```mermaid
graph LR
    META["TableMeta\nheapStart → Page 5"] --> P5["Page 5\n(slotted)\nnextPage → 8"]
    P5 --> P8["Page 8\n(slotted)\nnextPage → 14"]
    P8 --> P14["Page 14\n(slotted)\nnextPage → nil"]

    style META fill:#5b8c5a,color:#fff
```

### Row Encoding

You need to serialize a `Row` (map of column name → value) into bytes for the slotted page, and deserialize back. A simple approach:

```mermaid
flowchart LR
    ROW["Row\nid: 42\nname: 'Alice'\nage: 30"]
    ROW --> ENC["Encoder"]
    ENC --> BYTES["[NULL_BITMAP][col1_len][col1_data][col2_len][col2_data]..."]
    BYTES --> DEC["Decoder + Schema"]
    DEC --> ROW2["Row\nid: 42\nname: 'Alice'\nage: 30"]
```

Use a **null bitmap** at the front (one bit per column), then serialize each non-null column value. For fixed-width types (int32, int64, bool) just write the bytes. For variable-width (string, blob) prefix with a 2-byte length.

### Query Execution Path (Full Lifecycle)

```mermaid
sequenceDiagram
    participant Q as Query (e.g. SELECT WHERE id=42)
    participant TBL as TableLayer
    participant IDX as B+Tree Index
    participant SP as SlottedPage
    participant BM as BufferManager
    participant DM as DiskManager

    Q->>TBL: FindByIndex("users", "id", 42)
    TBL->>IDX: Search(key=42)

    loop Walk B+Tree (root → leaf)
        IDX->>BM: FetchPage(nodePageID)
        BM->>DM: ReadPage (if not in pool)
        DM-->>BM: raw bytes
        BM-->>IDX: *Page
        Note over IDX: Compare keys, pick child
        IDX->>BM: UnpinPage(nodePageID)
    end

    IDX-->>TBL: TupleID(pageID=14, slot=3)

    TBL->>BM: FetchPage(14)
    BM-->>TBL: *Page
    TBL->>SP: GetTuple(slot=3) on page bytes
    SP-->>TBL: raw bytes
    TBL->>TBL: Schema.Decode(bytes) → Row
    TBL->>BM: UnpinPage(14)
    TBL-->>Q: Row{id:42, name:"Alice", age:30}
```

### Scan Types

```mermaid
flowchart TD
    QUERY["WHERE clause"] --> PLAN{"Index available\nfor this column?"}
    PLAN -->|Yes| IXSCAN["Index Scan\n1. Search B+Tree → TupleID\n2. Fetch that one page\nO(log n)"]
    PLAN -->|No| SEQSCAN["Sequential Scan\n1. Walk every page in heap\n2. Decode every tuple\n3. Apply filter\nO(n)"]
```

### Design Decisions

1. **Catalog storage** — the catalog itself should be stored in the database (a "system table" on page 1). For v1, you can keep it in memory and rebuild on startup by reading a reserved page.
2. **Row encoding** — keep it dead simple. Don't worry about compression or columnar storage.
3. **Heap organisation** — a simple linked list of pages per table. Track the "last page with free space" so inserts are fast.
4. **Type system** — start minimal: `Int32`, `Int64`, `Text`, `Bool`. That's enough to be useful.

---

## 6 — SQL Layer

The top of the stack. Transforms human-readable SQL text into calls against the Table & Index layer.

### Pipeline

```mermaid
flowchart LR
    SQL["SELECT name FROM users\nWHERE age > 25\nORDER BY name"]
    SQL --> LEX["Lexer\n(Tokenizer)"]
    LEX --> TOKENS["[SELECT] [name] [FROM]\n[users] [WHERE] [age]\n[>] [25] [ORDER] [BY] [name]"]
    TOKENS --> PARSE["Parser"]
    PARSE --> AST["Abstract Syntax Tree"]
    AST --> PLAN["Planner /\nOptimiser"]
    PLAN --> EXEC["Executor"]
    EXEC --> RESULT["Result Rows"]

    style SQL fill:#4a6fa5,color:#fff
    style RESULT fill:#5b8c5a,color:#fff
```

### AST Structure

```mermaid
classDiagram
    class Statement {
        <<interface>>
    }

    class SelectStmt {
        +Columns []string
        +Table string
        +Where *Expr
        +OrderBy []OrderClause
        +Limit *int
    }

    class InsertStmt {
        +Table string
        +Columns []string
        +Values [][]Expr
    }

    class CreateTableStmt {
        +Name string
        +Columns []ColumnDef
    }

    class Expr {
        <<interface>>
    }

    class BinaryExpr {
        +Left Expr
        +Op Token
        +Right Expr
    }

    class LiteralExpr {
        +Value interface{}
    }

    class ColumnRef {
        +Name string
    }

    Statement <|-- SelectStmt
    Statement <|-- InsertStmt
    Statement <|-- CreateTableStmt
    Expr <|-- BinaryExpr
    Expr <|-- LiteralExpr
    Expr <|-- ColumnRef
    SelectStmt *-- Expr : Where
```

### Planner — Logical to Physical

```mermaid
flowchart TD
    AST["SelectStmt\ntable: users\nwhere: age > 25\norderBy: name"]

    AST --> LP["Logical Plan"]

    subgraph LP["Logical Plan"]
        direction TB
        PROJ["Project\n(name)"]
        SORT["Sort\n(name ASC)"]
        FILT["Filter\n(age > 25)"]
        SCAN["Scan\n(users)"]
        PROJ --> SORT --> FILT --> SCAN
    end

    LP --> OPT["Optimiser\n• Push filters down\n• Choose index vs seq scan"]

    OPT --> PP["Physical Plan"]

    subgraph PP["Physical Plan"]
        direction TB
        PROJ2["Project (name)"]
        SORT2["Sort (name ASC)"]
        IXSCAN2["IndexScan\n(users.age_idx, > 25)"]
        PROJ2 --> SORT2 --> IXSCAN2
    end
```

### Volcano / Iterator Execution Model

Every node in the physical plan is an **iterator** with a `Next() → Row` interface. Rows flow upward one at a time:

```mermaid
sequenceDiagram
    participant Client
    participant Proj as Project
    participant Sort as Sort
    participant Scan as IndexScan

    Client->>Proj: Next()
    Proj->>Sort: Next()

    Note over Sort: First call → pull ALL rows to sort
    loop Pull all rows
        Sort->>Scan: Next()
        Scan-->>Sort: Row (or EOF)
    end
    Note over Sort: Sort in memory

    Sort-->>Proj: Row (first sorted)
    Proj-->>Client: Row (projected columns only)

    Client->>Proj: Next()
    Proj->>Sort: Next()
    Sort-->>Proj: Row (second sorted)
    Proj-->>Client: Row (projected columns only)

    Client->>Proj: Next()
    Proj->>Sort: Next()
    Sort-->>Proj: EOF
    Proj-->>Client: EOF
```

### Supported SQL (Recommended Starting Set)

```mermaid
mindmap
  root((SQL Subset))
    DDL
      CREATE TABLE
      DROP TABLE
    DML
      SELECT
        WHERE
        ORDER BY
        LIMIT
      INSERT
      UPDATE ... WHERE
      DELETE ... WHERE
    Expressions
      Comparisons: = != < > <= >=
      Logical: AND OR NOT
      Literals: int, string, bool
```

### Design Decisions

1. **Lexer** — hand-write it. SQL tokens are simple enough that you don't need a generator. A `switch` on the current character with a `readWhile` helper covers 90% of cases.
2. **Parser** — recursive descent (one function per grammar rule: `parseSelect()`, `parseExpr()`, etc.). This is the most educational approach.
3. **No JOINs in v1.** Single-table queries are plenty to get the full stack working end-to-end.
4. **Optimiser** — start with two rules only: (a) choose index scan if an index exists on the WHERE column, (b) otherwise sequential scan. That's it.
5. **Error handling** — return clear error messages with the position in the SQL string where parsing failed.

---

## Suggested Build Order

```mermaid
flowchart TD
    M1["Milestone 1\nDisk Manager\n• Read/Write pages\n• Free list\n• Unit tests: alloc, free, persist"]
    M2["Milestone 2\nBuffer Manager\n• Buffer pool + page table\n• Pin/Unpin/Flush\n• Clock eviction\n• Test: eviction under pressure"]
    M3["Milestone 3\nSlotted Page\n• Insert/Get/Delete tuples\n• Page compaction\n• Test: fill page, delete, re-insert"]
    M4["Milestone 4\nB+Tree\n• Search, Insert, Split\n• Range scan via leaf chain\n• Test: insert 10k keys, verify order"]
    M5["Milestone 5\nTable & Index Layer\n• Schema, Catalog\n• Heap scan, Index scan\n• Row encode/decode\n• Test: CRUD on a table"]
    M6["Milestone 6\nSQL Layer\n• Lexer → Parser → AST\n• Planner + Executor\n• Wire it all together\n• Test: run SQL against your engine"]

    M1 --> M2 --> M3 --> M4 --> M5 --> M6

    style M1 fill:#555,color:#fff
    style M2 fill:#c2555a,color:#fff
    style M3 fill:#7a5195,color:#fff
    style M4 fill:#8b6f47,color:#fff
    style M5 fill:#5b8c5a,color:#fff
    style M6 fill:#4a6fa5,color:#fff
```

### Testing Strategy Per Milestone

| Milestone | Key Tests |
|---|---|
| **Disk Manager** | Allocate N pages, write data, close file, reopen, read back, verify. Free pages and reallocate — confirm reuse. |
| **Buffer Manager** | Fetch more pages than pool size — confirm eviction works. Pin a page, confirm it's never evicted. Dirty pages flushed on eviction. |
| **Slotted Page** | Fill a page to capacity. Delete from the middle. Insert again (reclaim space). Verify tuple data integrity round-trip. |
| **B+Tree** | Insert 10,000 sequential keys, then 10,000 random keys. Point lookups on all of them. Range scan and verify sorted order. Delete keys and verify. |
| **Table & Index** | Create table with schema. Insert 1,000 rows. Sequential scan with filter. Index lookup. Update and delete rows. |
| **SQL** | Parse valid and invalid SQL. Execute `CREATE TABLE`, `INSERT`, `SELECT ... WHERE`, `DELETE`. End-to-end: SQL string in → result rows out. |

---

## Reference: Complete Data Flow for `SELECT name FROM users WHERE id = 42`

```mermaid
flowchart TD
    SQL["SQL: SELECT name FROM users WHERE id = 42"]
    SQL --> LEX["Lexer → tokens"]
    LEX --> PARSE["Parser → SelectStmt AST"]
    PARSE --> PLAN["Planner: id has index → IndexScan"]
    PLAN --> EXEC["Executor calls TableLayer.FindByIndex"]

    EXEC --> BT["B+Tree.Search(42)"]
    BT --> BUF1["BufferMgr.FetchPage(root node)"]
    BUF1 --> DISK1["DiskMgr.ReadPage (if cache miss)"]
    DISK1 --> BUF1
    BUF1 --> BT
    BT --> BUF2["BufferMgr.FetchPage(leaf node)"]
    BUF2 --> BT
    BT --> TID["Found: TupleID(page=14, slot=3)"]

    TID --> BUF3["BufferMgr.FetchPage(14)"]
    BUF3 --> SLOT["SlottedPage.GetTuple(slot=3)"]
    SLOT --> DECODE["Schema.Decode → Row{id:42, name:'Alice', age:30}"]
    DECODE --> PROJECT["Project: keep only 'name'"]
    PROJECT --> RESULT["Result: 'Alice'"]

    style SQL fill:#4a6fa5,color:#fff
    style RESULT fill:#5b8c5a,color:#fff
```
