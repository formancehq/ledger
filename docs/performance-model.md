# Performance Model in Formance Ledger

## Introduction

The Formance Ledger is designed for high performance while maintaining data consistency and integrity. This document explains the performance characteristics of the Ledger, the factors that influence its throughput and latency, and how you can optimize it for your specific use case.

## Performance Fundamentals

Formance Ledger's performance model is built on several key principles:

1. **Feature-Based Configuration**: Performance is influenced by which features you enable
2. **Concurrency Control**: Optimized locking strategies to maximize throughput
3. **Database Efficiency**: Careful database design with appropriate indexes
4. **Scalability Options**: Multiple approaches to scale as your transaction volume grows

## Key Performance Metrics

When evaluating Ledger performance, these are the primary metrics to consider:

### Transaction Throughput

The Ledger's throughput is measured in transactions per second (TPS). Under optimal configurations, Formance Ledger can handle hundreds to thousands of transactions per second, depending on:

- Hardware resources
- Enabled features
- Transaction complexity
- Concurrent user load

### Transaction Latency

Transaction latency measures how long it takes for a single transaction to be processed. The Ledger provides several latency metrics:

- **Average latency**: Typical transaction processing time
- **P95/P99 latency**: Upper bounds for most transactions
- **Max latency**: Worst-case processing time

## Feature Impact on Performance

Different Ledger features have different performance implications:

### High-Impact Features

1. **HASH_LOGS (SYNC mode)**
   - **Impact**: Significant reduction in write throughput under high concurrency
   - **Reason**: Requires ledger-wide locking to maintain hash chain integrity
   - **Optimization**: Use ASYNC mode for better performance with eventual consistency

2. **MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES**
   - **Impact**: Moderate impact on write performance
   - **Reason**: Requires additional processing to maintain historical balance views
   - **Benefit**: Significantly improves read performance for historical balance queries

3. **INDEX_ADDRESS_SEGMENTS**
   - **Impact**: Slight impact on write performance
   - **Reason**: Maintains additional indexes
   - **Benefit**: Greatly improves query performance for segmented addresses

### Minimal Performance Configuration

For maximum throughput, you can use the minimal feature set:

```json
{
  "features": {
    "HASH_LOGS": "DISABLED",
    "MOVES_HISTORY": "OFF",
    "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES": "DISABLED",
    "INDEX_ADDRESS_SEGMENTS": "OFF",
    "INDEX_TRANSACTION_ACCOUNTS": "OFF"
  }
}
```

This configuration prioritizes raw transaction processing speed over advanced features.

## Concurrency and Performance

The Ledger's concurrency model is designed to maximize throughput while maintaining data consistency:

1. **Account-Level Locking**: Only transactions affecting the same accounts block each other
2. **Optimistic Operations**: Where possible, operations proceed without locking
3. **Smart Retries**: Automatic handling of temporary conflicts

This approach allows the system to scale effectively with the number of concurrent users, as long as they're operating on different accounts.

## Bucket Strategy for Performance

The Ledger's bucket system can be leveraged for performance optimization:

1. **Horizontal Isolation**: Each bucket operates independently, creating natural performance boundaries
2. **Workload Separation**: Place high-volume ledgers in dedicated buckets
3. **Resource Allocation**: Different buckets can be allocated different database resources

For example, you might create separate buckets for:
- High-volume operational transactions
- Analytical ledgers with complex queries
- Archive or compliance ledgers

## Performance Testing

Formance regularly benchmarks the Ledger using various scenarios:

1. **Single-transaction Throughput**: Maximum TPS for simple transactions
2. **Complex Transaction Performance**: Throughput with multi-posting transactions
3. **Concurrent User Scaling**: Performance under increasing user load
4. **Feature Impact Analysis**: Measuring the impact of each feature

These benchmarks help guide performance optimization and ensure the system meets demanding financial workloads.

## Performance Optimization Strategies

To get the best performance from Formance Ledger, consider these strategies:

### For Write-Heavy Workloads

1. **Feature Selection**: Enable only the features you need
2. **Bucket Strategy**: Separate high-write ledgers into dedicated buckets
3. **Batch Processing**: Use batch operations for bulk imports
4. **Transaction Design**: Optimize transaction structure (fewer postings when possible)

### For Read-Heavy Workloads

1. **Enable Indexing Features**: Use appropriate indexing features
2. **Query Optimization**: Leverage the targeted query capabilities
3. **Caching Strategy**: Implement application-level caching for frequent reads

## Hardware Considerations

The Ledger's performance is influenced by the hardware it runs on:

1. **Database Performance**: Fast disk I/O, sufficient memory, and CPU cores
2. **Network Latency**: Minimize network latency between application and database
3. **Scaling Options**: Vertical scaling (larger instances) or horizontal scaling (more buckets)

## Conclusion

Formance Ledger offers a flexible performance model that can be tailored to your specific needs. By understanding the impact of different features and optimizing your configuration, you can achieve the right balance between functionality and performance.

For high-throughput financial systems, the Ledger provides the tools to process hundreds to thousands of transactions per second while maintaining the consistency and auditability required for financial operations. 