import sqlite3
from dag_sqlite import DsTypes, DsObj

def ds_dump(cur: sqlite3.Cursor, obj_id: int) -> DsObj:
	obj_type, intval, blobval = cur.execute("SELECT ds_obj_type, ds_obj_val_int, ds_obj_val_blob FROM ds_obj WHERE ds_obj_id=?", (obj_id,)).fetchone()
	match DsTypes(obj_type):
		case DsTypes.NULL:
			return None
		case DsTypes.BOOLEAN:
			return bool(intval)
		case DsTypes.INTEGER:
			return intval & 0xffff_ffff_ffff_ffff
		case DsTypes.NEGATIVE_INTEGER:
			return ~(intval & 0xffff_ffff_ffff_ffff)
		case DsTypes.STRING:
			return blobval.decode()
		case DsTypes.BYTES:
			return blobval
		case DsTypes.LIST:
			return [
				ds_dump(cur, i) for i, *_ in
				cur.execute(
					"SELECT ds_arr_val FROM ds_arr WHERE ds_arr_id=? ORDER BY ds_arr_idx",
					(intval,)
				).fetchall()
			]
		case DsTypes.MAP:
			return {
				k.decode(): ds_dump(cur, v) for k, v in
				cur.execute(
					"SELECT ds_map_key, ds_map_val FROM ds_map WHERE ds_map_id=? ORDER BY length(ds_map_key), ds_map_key",
					(intval,)
				).fetchall()
			}
		case _:
			raise ValueError("unrecognised type")

if __name__ == "__main__":
	import os
	import json

	DB_PATH = "../test.db"

	# gross: CD to where the script is
	os.chdir(os.path.dirname(os.path.abspath(__file__)))

	con = sqlite3.connect(DB_PATH)
	cur = con.cursor()
	
	for root, *_ in cur.execute("SELECT ds_root_obj FROM ds_root"):
		obj = ds_dump(cur, root)
		print(json.dumps(obj, indent="  "))