# simpdb

## Example usage
```go
import "github.com/rprtr258/simpdb"

type User struct {
    Name string `json:"name"`
    Gender bool `json:"gender"`
}

// ID - get user ID. Must be unique among all users.
func (u User) ID() string {
    return u.Name
}

// TableName - get table name for user entities. Must be unique among all table
// names for all entities.
func (User) TableName() string {
    return "users"
}

func main() {
    db := simpdb.New("db")
    users := simpdb.GetTable[User](db)

    // get all users
    users, _ := users.GetAll()
    // get user by id
    user, _ := users.Get("alex")
    // get all male users
    males, _ := users.GetBy(func(u User) bool {
        return u.Gender
    })
    // insert new user
    _ := users.Insert(User{
        Name: "alex",
        Gender: true,
    })
    // insert new/update existing user
    _ := users.Upsert(User{
        Name: "mary",
        Gender: false,
    })
    // delete user by id
    _ := users.Delete("mary")
    // delete all females
    _ := users.DeleteBy(func(u User) bool {
        return !u.Gender
    })
}
```
