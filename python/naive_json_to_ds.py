import sqlite3
from typing import Dict, Any

from dag_sqlite import DsTypes, DsObj

# TODO: handle CIDs
# Note: This impl is recursive (ew) and not streamed (ew!)
def json_to_ds_obj(cur: sqlite3.Cursor, obj: DsObj) -> int:
	"""
	creates a new DAG-SQLite object, and returns its ds_obj_id
	"""

	match obj:
		case None:
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type) VALUES (?)",
				(DsTypes.NULL.value,)
			)
			return cur.lastrowid
		case bool():
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type, ds_obj_val_int) VALUES (?, ?)",
				(DsTypes.BOOLEAN.value, obj)
			)
			return cur.lastrowid
		case int():
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type, ds_obj_val_int) VALUES (?, ?)",
				(DsTypes.INTEGER.value, obj)
			)
			return cur.lastrowid
		case str():
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type, ds_obj_val_blob) VALUES (?, ?)",
				(DsTypes.STRING.value, obj.encode())
			)
			return cur.lastrowid
		case bytes() | bytearray():
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type, ds_obj_val_blob) VALUES (?, ?)",
				(DsTypes.BYTES.value, obj)
			)
			return cur.lastrowid
		case list():
			# TODO: improve id selection
			if obj:
				arr_id = (cur.execute("SELECT MAX(ds_arr_id) FROM ds_arr").fetchone()[0] or 0) + 1
				# this is a hack to make sure nested lists will "see" the new ID
				cur.execute("INSERT INTO ds_arr(ds_arr_id, ds_arr_idx, ds_arr_val) VALUES (?, 0, 0)", (arr_id,))
			else:
				arr_id = None
			for i, value in enumerate(obj):
				val_id = json_to_ds_obj(cur, value)
				cur.execute(
					"INSERT OR REPLACE INTO ds_arr(ds_arr_id, ds_arr_idx, ds_arr_val) VALUES (?, ?, ?)",
					(arr_id, i, val_id)
				)
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type, ds_obj_val_int) VALUES (?, ?)",
				(DsTypes.LIST.value, arr_id)
			)
			return cur.lastrowid
		case dict():
			# TODO: improve id selection
			if obj:
				map_id = (cur.execute("SELECT MAX(ds_map_id) FROM ds_map").fetchone()[0] or 0) + 1
				# this is a hack to make sure nested dicts will "see" the new ID
				cur.execute("INSERT INTO ds_map(ds_map_id, ds_map_key, ds_map_val) VALUES (?, ?, 0)", (map_id, next(iter(obj)).encode()))
			else:
				map_id = None
			for key, value in obj.items():
				if type(key) is not str:
					raise TypeError("map keys must be strings")
				val_id = json_to_ds_obj(cur, value)
				cur.execute(
					"INSERT OR REPLACE INTO ds_map(ds_map_id, ds_map_key, ds_map_val) VALUES (?, ?, ?)",
					(map_id, key.encode(), val_id)
				)
			cur.execute(
				"INSERT INTO ds_obj(ds_obj_type, ds_obj_val_int) VALUES (?, ?)",
				(DsTypes.MAP.value, map_id)
			)
			return cur.lastrowid
		case _:
			raise TypeError(f"unsupported type: {type(obj)}")



if __name__ == "__main__":
	import os
	import json

	DB_PATH = "../test.db"
	SCHEMA_PATH = "../dag_sqlite.sql"

	# gross: CD to where the script is
	os.chdir(os.path.dirname(os.path.abspath(__file__)))

	# blow away any previous db
	try:
		os.remove(DB_PATH)
	except OSError:
		pass

	con = sqlite3.connect(DB_PATH)
	cur = con.cursor()

	# init the db with our schema
	with open(SCHEMA_PATH) as f:
		cur.executescript(f.read())
	
	obj_id = json_to_ds_obj(cur, json.load(open("../test_data/basics.json")))
	cur.execute("INSERT INTO ds_root(ds_root_obj) VALUES (?)", (obj_id,))

	con.commit()

	# sqlite3 test.db .dump