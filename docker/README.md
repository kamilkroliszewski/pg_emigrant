# Docker test environments

## For the automated suite: nothing to do

`pytest tests/integration` starts and stops its own PostgreSQL containers. It
needs a working Docker daemon and nothing else — no compose file, no prior
`up`, no cleanup. Containers are labelled `pg_emigrant_test=1`, so a run
interrupted mid-way can be tidied with:

```bash
docker rm -f $(docker ps -aq --filter label=pg_emigrant_test=1)
```

Version pairs are selected on the command line:

```bash
pytest tests/integration                                    # 18 -> 18
pytest tests/integration --pg-matrix "14->18,17->17"
pytest tests/integration --pg-matrix full                    # the whole matrix
```

The suite gives its containers the **host network namespace** rather than
publishing ports. That is a correctness requirement, not a convenience: a
subscription's `CONNECTION` string is stored verbatim and resolved later by the
*target's own apply worker*. With published ports, `127.0.0.1:<source port>`
means "the source" to the test process and "myself" to the target container —
which is the exact production failure mode pg_emigrant warns about, and it
would make every replication test fail for a reason unrelated to what it tests.

## For working on the tool by hand: `docker-compose.yml`

Brings up one long-lived server per supported major version, all configured for
logical replication, so you can point a real `config.yaml` at them.

```bash
export PG_EMIGRANT_TEST_PG_PASSWORD=$(openssl rand -hex 16)
docker compose -f docker/docker-compose.yml up -d
```

| Version | Port |
|---|---|
| 14 | 15432 |
| 15 | 15433 |
| 16 | 15434 |
| 17 | 15435 |
| 18 | 15436 |

```yaml
# config.yaml — a 14 → 18 migration against those servers
source: {host: 127.0.0.1, port: 15432, user: postgres, password: "…", dbname: postgres}
target: {host: 127.0.0.1, port: 15436, user: postgres, password: "…", dbname: postgres}
```

> Note the caveat above: because these containers *do* publish ports, a
> subscription created against them stores `host=127.0.0.1`, which the target
> container resolves to itself. Schema sync, the data copy and every read-only
> command work fine; `CREATE SUBSCRIPTION` will not stream. Put both containers
> on the host network, or use a container name reachable from both, if you need
> the replication half to run.

Tear down, including the data volumes:

```bash
docker compose -f docker/docker-compose.yml down -v
```
