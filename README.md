# Stor
Stor is a library for implementing embedded KV database based on RocksDB. It was originally based on [heed](https://github.com/meilisearch/heed), 
but has been since fully rewritten from that codebase. It's designed to support multiple backends. Currently only RocksDB backend is implemented. 

The core storage engine of [Blok3](https://blok3.io) is built on multiple `stor`-based databases.

# Example
A simple example in which we create database with generic backend, with one table storing `email -> User` mapping with different encoding formats for 
key and value.
```rust
#[derive(Debug, Default, Clone, PartialEq, Proto)]
pub struct User {
  #[field(1, string, singular)]
  pub name: String,
}

pub struct DB<'s, S: Store> {
    // Maps email to users
    pub users: Typed<'s, S, Str, Protokit<User>>
}

pub fn tables<S: Store>(s: S) -> Arc<stor::Tables<S, DB<'static, S>>> {
  Ok(Arc::new(Tables::new(s, |store| DB {
      users: store.typed("users")?,
  })?))
}

pub fn register<S: Store>(db: &Tables<S, DB<'static, S>>, mail: &str, name: &str) {
  db.store.with_wtx(|wtx| {
    db.users.put(wtx, &mail, &User {
      name: name.to_string(),
    })
  }).unwrap()
}
```
