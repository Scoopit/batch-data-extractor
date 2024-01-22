# batch-data-extractor

Extract data from multiple parquet (or csv) files and
output it as `INSERT INTO` sql statements.

Take a YAML config file as input.

````YAML
queries:
  - name: user
    sql: SELECT * FROM user WHERE lid=1
  - name: post
    sql: SELECT * FROM post WHERE curatedby_lid=1
  - name: extract exts...
    table_name: post_ext
    sql: |
      SELECT pe.* FROM post p 
      JOIN post_ext pe ON p.lid=pe.lid
      WHERE p.curatedby_lid=1
````

## Installation

````bash
cargo install --git https://github.com/Scoopit/batch-data-extractor.git
````

## Credits

Some part of the code (utils.rs) have been brutally
imported  from [Boring Data Tool (bdt)](https://github.com/andygrove/bdt/tree/main) as
using bdt as a lib is not really possible due to some
type extravagance (errors not implementing `Error` trait)...

## License

Licensed under Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
