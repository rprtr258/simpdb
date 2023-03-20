[![GoDoc][doc-img]][doc]

# simpdb

## Example usage
See [db_test.go](./db_test.go) for example usage.

## Implementing own storage
You can implement your own file storage format, if you want to. See examples in [storages](./storages) directory for already implemented file storages.

## Method chaining

```mermaid
flowchart LR
  T[Table]
  MSE[map string E]
  SE[slice E]
  OE[Optional E]
  Q[select]
  L[list]
  T -->|Get id| r1[Optional E]
  T -->|Insert E| bool
  T -->|Upsert Es| r2[void]
  T -->|DeleteByID id| bool
  T --> Q
  subgraph select
    Q -->|All| MSE
    Q -->|List| L
    Q -->|Sort less| L
    Q -->|Where filter| Q
    Q -->|Delete| SE
    Q -->|Update fn| void
    Q -->|Count| int
    Q -->|Iter| r3[void]
    subgraph list
        L -->|Sort less| L
        L -->|All| SE
        L -->|Min| OE
        L -->|Max| OE
        L -->|Iter| r4[void]
    end
  end
```

[doc-img]: https://pkg.go.dev/badge/github.com/rprtr258/simpdb
[doc]: https://pkg.go.dev/github.com/rprtr258/simpdb
