env "local" {
  src = "file://internal/db/schema.sql"
  dev = "sqlite://file?mode=memory&_fk=1"

  migration {
    dir = "file://internal/db/migrations"
  }
}
