Benchmarking
============

Environment
-----------

The machine running tests is Linux CentOS 2.6.18-194.32.1.el5xen #1 SMP with
Quad Core Xeon X3360 @ 2.83GHz, 4GB RAM.

Postgres server version: 9.0.3
Postgres pqlib version: 9.3

Fully Asynchronous vs. poor man's async
---------------------------------------

The following {file:benchmarks/em_pg.rb benchmark} compares fully
asynchronous implementation (`em-pg-client`) versus blocking em-pg drivers.

The goal of the test is to retrieve (~80000) rows from the same table with
a lot of text data, in chunks, using parallel connections.

The parallel method uses synchrony for simplicity.

* `single` is (eventmachine-less) job for retrieving a whole data table in
  one simple query "select * from resources"
* `parallel` chunk_row_count / concurrency] uses em-pg-client for retrieving
  result in chunks by `chunk_row_count` rows and using `concurrency` parallel
  connections
* `blocking` chunk_row_count / concurrency is similiar to `parallel` except
  that it uses special patched version of library that uses blocking
  PGConnection methods


```
    >> benchmark 1000
                              user     system      total        real
    single:              80.970000   0.350000  81.320000 (205.592592)

    parallel 90000/1:    87.380000   0.710000  88.090000 (208.171564)
    parallel 5000/5:     84.250000   3.760000  88.010000 (141.031289)
    parallel 2000/10:    90.190000   4.970000  95.160000 (152.844950)
    parallel 1000/20:    97.070000   5.390000 102.460000 (212.358631)

    blocking 90000/1:    93.590000   0.610000  94.200000 (230.190776)
    blocking 5000/5:     79.930000   1.810000  81.740000 (223.342432)
    blocking 2000/10:    76.990000   2.820000  79.810000 (225.347169)
    blocking 1000/20:    78.790000   3.230000  82.020000 (225.949107)
```

As we can see the gain from using asynchronous em-pg-client while
using `parallel` queries is noticeable (up to ~30%).

The `blocking` client however doesn't gain much from parallel execution.
This was expected because it freezes eventmachine until the whole
dataset is consumed by the client.


Threads vs. Fibers Streaming Benchmark
--------------------------------------

The following {file:benchmarks/single_row_mode.rb benchmark} compares
performance of parallel running threads using vanilla PG::Connection driver
versus EventMachine driven parallel Fibers using PG::EM::Client v0.3.2.

Each thread/fiber retrieves first 5000 rows from the same table with
a lot of text data in a `single_row_mode`. After 5000 rows is retrieved
the connection is being reset. The process is repeated after all parallel
running threads/fibers finish their task.

Both Thread and Fiber versions use the same chunk of code to retrieve rows.

```
    >> benchmark 400
                               user     system      total        real
    threads 400x1:        24.970000   1.090000  26.060000 ( 30.683818)
    threads 80x5:         24.730000   7.020000  31.750000 ( 51.402710)
    threads 40x10:        22.880000   7.460000  30.340000 ( 52.548910)
    threads 20x20:        22.220000   7.130000  29.350000 ( 53.911111)
    threads 10x40:        22.570000   7.620000  30.190000 ( 54.111841)

    fibers  400x1:        26.040000   1.060000  27.100000 ( 31.619598)
    fibers  80x5:         28.690000   1.140000  29.830000 ( 33.025573)
    fibers  40x10:        28.790000   1.280000  30.070000 ( 33.498418)
    fibers  20x20:        29.100000   1.210000  30.310000 ( 33.289344)
    fibers  10x40:        29.220000   1.340000  30.560000 ( 33.691188)
```

```
    AxB - repeat A times running B parallel threads/fibers.
```

