# Haskell Dataframe Benchmark

This benchmark entry uses Haskell with the Cassava CSV library to implement dataframe operations.

## Implementation Details

- **Language**: Haskell (GHC)
- **CSV Library**: Cassava
- **Build Tool**: Stack

## Implemented Benchmarks

### Groupby (`groupby-haskell.hs`)
Implements 5 out of 10 groupby questions:
1. sum v1 by id1
2. sum v1 by id1:id2
3. sum v1 mean v3 by id3
4. mean v1:v3 by id4
5. sum v1:v3 by id6

Note: Questions 6-10 would require additional statistical functions (median, standard deviation, regression, top-n selection).

### Join (`join-haskell.hs`)
Implements 1 out of 5 join questions:
1. small inner on int

Note: Additional join questions would require implementation of outer joins and multi-key joins.

## Setup

Run the setup script to install dependencies:
```bash
./haskell/setup-haskell.sh
```

## Notes

This is a basic implementation demonstrating Haskell's capabilities for dataframe operations. The implementation uses:
- Strict evaluation with NFData for proper benchmarking
- HashMap-based grouping for efficient aggregations
- Standard Haskell list operations for data processing

For production use, consider using more specialized libraries like:
- Frames (type-safe dataframes)
- HaskellDB (database integration)
- Haskelltable (in-memory data tables)
