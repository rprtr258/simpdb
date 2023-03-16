[![GoDoc][doc-img]][doc]

# simpdb

## Example usage
```go
import "github.com/rprtr258/simpdb"

type User struct {
    Name   string `json:"name"`
    Gender bool `json:"gender"`
    Age    int `json:"age"`
}

// ID - get user ID. Must be unique among all users.
func (u User) ID() string {
    return u.Name
}

func main() {
    db := simpdb.New("db")

    users, _ := simpdb.GetTable[User](db, "users", simpdb.TableConfig{})
    defer users.Flush()

    // get all users as map name -> user
    users := users.All()

    // get all users as list sorted by name
    users := users.List().All()

    // get user by id
    user := users.Get("Harry").Value

    // get all male users
    males := users.
        Where(func(_ string, u User) bool {
            return u.Gender
        }).
        All()

    // insert new user, if not already exists
    didInsert := users.Insert(User{
        Name: "Harry",
        Gender: true,
        Age: 20,
    })

    // insert new (update if already exist) user
    users.Upsert(User{
        Name: "Hermione",
        Gender: false,
        Age: 19,
    })

    // flush file to disk
    _ = users.Flush()

    // delete user by id
    didDelete := users.DeleteByID("Hermione")

    // delete all females
    femalesDeleted := users.
        Where(func(_ string, u User) bool {
            return !u.Gender
        }).
        Delete()

    // male users sorted by age
    wizards := users.
        Where(func(_ string, u User) bool {
            return u.Gender
        }).
        Sort(func(u1, u2 User) bool {
            return u1.Age < u2.Age
        }).
        All()

    // most elder woman
    macGonagall, ok := users.
        Where(func(_ string, u User) bool {
            return !u.Gender
        }).
        Sort(func(u1, u2 User) bool {
            return u1.Age < u2.Age
        }).
        Max()

    // make everyone female
    users.Update(func(u User) User {
        u.Gender = false
        return u
    })

    // update single person
    users.
        Where(func(id string, _ User) bool {
            return id == "Harry"
        }).
        Update(func(u User) User {
            u.Gender = false
            return u
        })
    // or
    user := users.Get("Harry").Value
    user.Gender = false
    users.Upsert(user)
}
```

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
