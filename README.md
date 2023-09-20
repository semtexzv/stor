# Stor
Stor is a library for building embedded databases. It works on underlying transactional KV storage, and provides typed interfaces, and a 
way to be generic over the underlying storage engine. Currently, a RocksDB backend is implemented. 

The core storage engine of [Blok3](https://blok3.io) is built on multiple `stor`-based databases.

# Example
A simple example in which we create database with generic backend, with one table storing `email -> User` mapping with different encoding formats for 
key and value.
```rust
#[derive(Debug, Default, Clone, PartialEq, Proto)]
pub struct UserData {
  #[field(1, string, singular)]
  pub name: String,
  #[field(2, bytes, optional)]
  pub hash: [u8; 32]
}

/// Container holding all the tables that we're interested in.
pub struct DB<'s, S: Store> {
    /// Users table maps emails to data about individual users
    pub users: Typed<'s, S, Str, Protokit<UserData>>
}
/// The `stor::Tables` holds both the storage engine, and the table references
/// in a safe way (self-referential lifetimes).
pub fn tables<S: Store>(s: S) -> Arc<stor::Tables<S, DB<'static, S>>> {
  Ok(Arc::new(Tables::new(s, |store, cfg| DB {
      users: store.typed("users", &cfg)?,
  })?))
}
/// Using the database requires transactions
pub fn register<S: Store>(db: &Tables<S, DB<'static, S>>, mail: &str, name: &str) {
  db.store.with_wtx(|wtx| {
    db.users.put(wtx, &mail, &User {
      name: name.to_string(),
      hash: None,
    })
  }).unwrap()
}
```

### Data formats
You can select different format for key and value of every table. The built-in ones are:

- [zerocopy](https://docs.rs/zerocopy/latest/zerocopy/) based types, and their slices.
- raw slices and strings,
- protobuf using [protokit](https://github.com/semtexzv/protokit)
- [json](https://github.com/serde-rs/json)
- [ordcode](https://github.com/pantonov/ordcode) - Useful for fully ordered keys.
- [postcard](https://github.com/jamesmunns/postcard)
