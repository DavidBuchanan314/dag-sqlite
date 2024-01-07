/*

Terminology:

"Map" is a key/value mapping (as in IPLD). Specifically, it maps UTF8 string keys to Objects

"Object" is a value with associated type.

Each type has a numeric ID:

0: null     (no associated value)
1: boolean  (value stored as INTEGER)
2: integer  (value stored as INTEGER)
3: string   (value stored as UTF8 BLOB)
4: bytes    (value stored as BLOB)
5: list     (value stored as INTEGER (a foreign key reference))
6: map      (value stored as INTEGER (likewise))
7: link     (value stored as BLOB)

*/


/* core data model */

CREATE TABLE ds_obj (
	ds_obj_id INTEGER PRIMARY KEY,
	ds_obj_type INTEGER NOT NULL,
	ds_obj_val_int INTEGER, /* nullable, depending on ds_obj_type */
	ds_obj_val_blob BLOB /* likewise */
);

CREATE TABLE ds_arr (
	ds_arr_id INTEGER NOT NULL,
	ds_arr_idx INTEGER NOT NULL,
	ds_arr_val INTEGER NOT NULL, /* reference to a ds_obj_id */

	PRIMARY KEY (ds_arr_id, ds_arr_idx)
);

CREATE TABLE ds_map (
	ds_map_id INTEGER NOT NULL,
	ds_map_key BLOB NOT NULL, /* UTF-8 encoded string */
	ds_map_val INTEGER NOT NULL, /* reference to a ds_obj_id */

	PRIMARY KEY (ds_map_id, ds_map_key)
);

/* TODO: add an index to assist with DAG-CBOR map key sort order? */


/* extras */

/* This could allow using an .sqlite file a bit like a CAR file. */
/* If I want to take that idea seriously, I should consider a version field somewhere. */
CREATE TABLE ds_root (
	ds_root_obj INTEGER NOT NULL
)


/* temporary state */

/*

TODO: figure this out.

When parsing nested objects, we need to keep track of the parse state in a stack-like data structure.

I'll cross that bridge when I come to it.

*/