env "local" {
  src = "file://db/schema.sql"
  dev = "sqlite://file?mode=memory&_fk=1"

  migration {
    dir = "file://db/migrations"
  }
}
