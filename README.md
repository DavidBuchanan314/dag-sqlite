# DAG-SQLite

DAG-SQLite is (or, will be) an SQLite-based represention of the IPLD Data Model.

## Why?

For working with arbitrarily large objects in a memory-constrained environment, you inevitably need to buffer them to disk. SQLite is a convenient mechanism for doing so. In particular, I expect it will be convenient for enforcing DAG-CBOR's map key order canonicalization rules, using SQLite to do the heavy lifting for sorting.

The goal is not *performance*, but efficient use of constrained resources, without arbitrarily limiting the size or shape of processable data.

I aim to write code that has O(1) memory consumption, but at the cost of O(n) disk space. This also implies using non-recursive algorithms (or at least, moving recursive state onto disk, rather than encoding it within the native call stack).

## Status

This repo will eventually contain: (roughly in planned order)

- [ ] A definition of the required SQLite tables, and a description of how they're used to encode the IPLD Data Model.
- [ ] Non-streamed Python code that converts between JSON and DAG-SQLite (as a reference/prototype).
- [ ] Python code for streaming (DAG-)JSON into a DAG-SQLite representation.
- [ ] Python code for streaming DAG-SQLite into a DAG-CBOR representation.
- [ ] Python code for streaming DAG-SQLite into a (DAG-)JSON representation.
- [ ] Python code for streaming DAG-CBOR into a (DAG-)JSON representation (Note! This one doesn't actually need DAG-SQLite as an IR).
- [ ] C code that matches the functionality of the (streaming) Python code.

The Python code mainly exists for prototyping and testing, while the C version is the real goal. Since they'll both be using the same SQLite representation, they'll be intercompatible.

By the way, I don't plan on supportting floating-point types, for now.