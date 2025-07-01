# ScyllaDB Performance Best Practices - Python

[![asciicast](https://asciinema.org/a/lbDlc0PEC3HUjhfCGiFenEdWY.svg)](https://asciinema.org/a/lbDlc0PEC3HUjhfCGiFenEdWY)

This repository shows how to achieve best performance results when using [ScyllaDB's Python Driver](https://pypi.org/project/scylla-driver/) (`scylla-driver`). 

Due to the way Python works, the code example in this repository makes use of [PyPy](https://pypy.org/). When working with Python, one challenge users may stumble is low throughput due to the Python [GIL](https://wiki.python.org/moin/GlobalInterpreterLock). As noted under the driver's [Performance Notes](https://python-driver.docs.scylladb.com/stable/performance.html): 

> Due to the GIL and limited concurrency, the driver can become CPU-bound pretty quickly. The sections below discuss further runtime and design considerations for mitigating this limitation.
> (...) redated
> Be sure to **never share any** `Cluster`, `Session`, or `ResponseFuture` **objects across multiple processes**. These objects should all be created after forking the process, not before.

Interestingly enough, sometimes even following these best practices is not enough to achieve high throughput, at which point you should consider:
- Using an alternative like PyPy
- Scale the clients to different hosts
- Use an alternative programming language

Using PyPy as demonstrated here can easily reach over 400K operations per second under a decent loader (we used `c5n.9xlarge`) and a small 3-node ScyllaDB cluster (we used `i4i.4xlarge`).

# Installation

- Install `uv` in case you don't have it already: https://docs.astral.sh/uv/getting-started/installation/
- Clone this repo
- `uv run program.py --help`:

```shell
$ uv run program.py --help
usage: program.py [-h] [--dc DC] [--keyspace KEYSPACE] [--compression] [--concurrency CONCURRENCY] [--processes PROCESSES]
                  [--operations OPERATIONS] [--blob-size BLOB_SIZE] [--mode {write,read,mixed}] [--read-ratio READ_RATIO]
                  address

positional arguments:
  address               ScyllaDB address

optional arguments:
  -h, --help            show this help message and exit
  --dc DC               DC to send queries to (datacenter1)
  --keyspace KEYSPACE   Keyspace name (test)
  --compression         Enable LZ4 compression (true)
  --concurrency CONCURRENCY
                        Concurrency per process. (1)
  --processes PROCESSES
                        Number of processes. (core count)
  --operations OPERATIONS
                        Total operations (1M)
  --blob-size BLOB_SIZE
                        Size of each blob in bytes to write (1_000)
  --mode {write,read,mixed}
  --read-ratio READ_RATIO
                        Proportion of read operations in mixed mode (0.5)
```

