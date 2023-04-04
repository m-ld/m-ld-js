SparseArray
- all infinite positions exist
- no splicing
- (sparse array with base index)

TSeq
- is a TSeqNode with a fixed null character
- has process metadata (current & vector)

TSeqNode
- is a char (call be null, ☐)
- is a map of process ID to SparseArray<TSeqNode>
- interpreted as: char concat each process ID's TSeqNode array
- a process can edit the char of its own TSeqNodes, and nested arrays
- can be represented as a char
- SparseArray<TSeqNode> can be represented as a string if base is 0

Example JSON
```json
{
  "char": "☐",
  "a": {
    "base": 0,
    "0": {
      "char": "h",
      "a": "☐",
      "b": [
        {
          "char": "i",
          "a": " world"
        }
      ]
    }
  },
  "b": "☐☐"
}
```

InsertAfter(ts) := 
1. IF owner of array, and no sub-nodes, and next slot available, set it
2. ELSE IF next char is at same level, add to node subarray
3. ELSE prepend to array with next char (at any level)

DeleteAt(ts) :=
1. Set char to ☐

PosId
- Is a path into the TSeq, e.g. a/0/b/0
- Process IDs are aliased to an integer

TSeqString
- Array of TSeqNode refs
- Maintains sequence in face of API & remote ops

Boils down to OwnedRegister:
- owner has a tick
- owner can set, at new tick
- anyone can unset, at last-seen tick

| A (owner)    | B   | C          |
|--------------|-----|------------|
| a@1          |     |            |
|              | a@1 | a@1        |
| b@2          | ☐@1 |            |
| -ignore ☐@1- | b@2 | b@2 or ☐@1 |
|              |     | b@2        |
| ☐@3          |     | ☐@2        |
|              | ☐@3 |            |

- Requires causal delivery OR unset-wins, to avoid ordering anomalies:

| A    | B   | C             |
|------|-----|---------------|
| a@1  |     |               |
|      | a@1 |               |
|      | ☐@1 |               |
| ☐@1  |     | ☐@1           |
|      |     | -a@1 arrives- |