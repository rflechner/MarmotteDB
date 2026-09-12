![MarmotteDB](docs/images/github_title_with_mascotte.png)

# MarmotteDB

[Version française](README.fr.md)

> [!WARNING]
> MarmotteDB is not finished. It is a proof of concept (POC) under active
> development and is not ready for production use.

## REST API

Start the server from the `marmotte-server` folder:

```powershell
cargo run
```

It listens on `http://127.0.0.1:7474` and exposes three endpoints:

| Method | Path | Description |
| ------ | ---- | ----------- |
| POST | `/databases` | Create a database |
| POST | `/databases/{database}/collections` | Create a collection |
| POST | `/databases/{database}/collections/{collection}/documents` | Store a JSON document |

The OpenAPI 3.1 specification is generated at compile time from the handlers by
[utoipa](https://github.com/juhaku/utoipa), so it cannot drift from the code.

- Swagger UI: <http://127.0.0.1:7474/swagger-ui/>
- OpenAPI document: <http://127.0.0.1:7474/api-docs/openapi.json>

## Indexing algorithms

MarmotteDB is intended, among other things, to experiment with and compare
several indexing algorithms. Different approaches will be implemented and tested
throughout development to evaluate their performance, resource consumption, and
trade-offs for different use cases.

### Sorted Index Table

![Sorted Index Table diagram](docs/images/sorted-index-table.png)

The Sorted Index Table is a fragment-based sorted index currently under
experimentation. Its on-disk format, API, and persistence limitations are
described in the [dedicated documentation](docs/sorted-index-table.md).

Run the tests from the repository root:

```powershell
cargo test --offline --manifest-path marmotte-server/Cargo.toml
```
