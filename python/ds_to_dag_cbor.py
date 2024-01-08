import sqlite3
from dag_sqlite import DsTypes, DsObj
from typing import BinaryIO
from enum import Enum

# XXX: these are different to the type codes used by DAG-SQLite!
class CborMajorType(Enum):
	INTEGER = 0
	NEGATIVE_INTEGER = 1
	BYTE_STRING = 2
	TEXT_STRING = 3
	ARRAY = 4
	MAP = 5
	TAG = 6
	SIMPLE = 7 # aka float, but we don't support floats


def write_dag_cbor_varint(stream: BinaryIO, major_type: CborMajorType, value: int) -> None:
	if value < 24:
		stream.write(bytes([major_type.value << 5 | value]))
	elif value < (1<<8):
		stream.write(bytes([major_type.value << 5 | 24]))
		stream.write(value.to_bytes(1, "big"))
	elif value < (1<<16):
		stream.write(bytes([major_type.value << 5 | 25]))
		stream.write(value.to_bytes(2, "big"))
	elif value < (1<<32):
		stream.write(bytes([major_type.value << 5 | 26]))
		stream.write(value.to_bytes(4, "big"))
	else:
		stream.write(bytes([major_type.value << 5 | 27]))
		stream.write(value.to_bytes(8, "big"))

def calc_dag_cbor_varint_size(value: int):
	if value < 24:
		return 1
	if value < (1<<8):
		return 2
	if value < (1<<16):
		return 3
	if value < (1<<32):
		return 5
	return 9

def ds_to_dag_cbor(stream: BinaryIO, cur: sqlite3.Cursor, obj_id: int) -> None:
	stack = [(None, obj_id)]
	while stack:
		key_string, obj_id = stack.pop()
		if key_string is not None: # special case for map keys
			write_dag_cbor_varint(stream, CborMajorType.TEXT_STRING, len(key_string))
			stream.write(key_string)
		obj_type, intval, blobval = cur.execute("SELECT ds_obj_type, ds_obj_val_int, ds_obj_val_blob FROM ds_obj WHERE ds_obj_id=?", (obj_id,)).fetchone()
		match DsTypes(obj_type):
			case DsTypes.NULL:
				write_dag_cbor_varint(stream, CborMajorType.SIMPLE, 22)
			case DsTypes.BOOLEAN:
				write_dag_cbor_varint(stream, CborMajorType.SIMPLE, 20 + intval)
			case DsTypes.INTEGER:
				write_dag_cbor_varint(stream, CborMajorType.INTEGER, intval & 0xffff_ffff_ffff_ffff)
			case DsTypes.NEGATIVE_INTEGER:
				write_dag_cbor_varint(stream, CborMajorType.NEGATIVE_INTEGER, intval & 0xffff_ffff_ffff_ffff)
			case DsTypes.STRING:
				write_dag_cbor_varint(stream, CborMajorType.TEXT_STRING, len(blobval))
				stream.write(blobval)
			case DsTypes.BYTES:
				write_dag_cbor_varint(stream, CborMajorType.BYTE_STRING, len(blobval))
				stream.write(blobval)
			case DsTypes.LIST:
				arrlen = cur.execute("SELECT COUNT(*) FROM ds_arr WHERE ds_arr_id=?", (intval,)).fetchone()[0]
				write_dag_cbor_varint(stream, CborMajorType.ARRAY, arrlen)
				arritems = [
					row[0] for row in
					cur.execute(
						"SELECT ds_arr_val FROM ds_arr WHERE ds_arr_id=? ORDER BY ds_arr_idx",
						(intval,)
					).fetchall()
				][::-1]
				stack += [(None, x) for x in arritems]
			case DsTypes.MAP:
				maplen = cur.execute("SELECT COUNT(*) FROM ds_map WHERE ds_map_id=?", (intval,)).fetchone()[0]
				write_dag_cbor_varint(stream, CborMajorType.MAP, maplen)
				mapitems = cur.execute(
					"SELECT ds_map_key, ds_map_val FROM ds_map WHERE ds_map_id=? ORDER BY length(ds_map_key), ds_map_key",
					(intval,)
				).fetchall()[::-1]
				stack += mapitems

			case _:
				raise ValueError("unrecognised type")



def ds_to_dag_cbor_recursionless(stream: BinaryIO, cur: sqlite3.Cursor, obj_id: int) -> None:
	# temp hack to init the stack: (final version will grow ad hoc) (or maybe a preflight scan will already know?)
	for i in range(100):
		cur.execute("INSERT OR REPLACE INTO ds_stack(ds_stack_idx, ds_stack_val) VALUES (?, 0)", (i,))
	
	cur.execute("INSERT OR REPLACE INTO ds_stack(ds_stack_idx, ds_stack_val) VALUES (0, ?)", (obj_id,))
	stack_ptr = 0
	while stack_ptr >= 0:
		key_string, obj_type, intval, blobval = cur.execute("""
			SELECT ds_stack_key, ds_obj_type, ds_obj_val_int, ds_obj_val_blob
			FROM ds_stack INNER JOIN ds_obj ON ds_obj_id=ds_stack_val
			WHERE ds_stack_idx=?
		""", (stack_ptr,)).fetchone()
		stack_ptr -= 1
		if key_string is not None: # special case for map keys
			write_dag_cbor_varint(stream, CborMajorType.TEXT_STRING, len(key_string))
			stream.write(key_string)

		match DsTypes(obj_type):
			case DsTypes.NULL:
				write_dag_cbor_varint(stream, CborMajorType.SIMPLE, 22)
			case DsTypes.BOOLEAN:
				write_dag_cbor_varint(stream, CborMajorType.SIMPLE, 20 + intval)
			case DsTypes.INTEGER:
				write_dag_cbor_varint(stream, CborMajorType.INTEGER, intval & 0xffff_ffff_ffff_ffff)
			case DsTypes.NEGATIVE_INTEGER:
				write_dag_cbor_varint(stream, CborMajorType.NEGATIVE_INTEGER, intval & 0xffff_ffff_ffff_ffff)
			case DsTypes.STRING:
				write_dag_cbor_varint(stream, CborMajorType.TEXT_STRING, len(blobval))
				stream.write(blobval)
			case DsTypes.BYTES:
				write_dag_cbor_varint(stream, CborMajorType.BYTE_STRING, len(blobval))
				stream.write(blobval)
			case DsTypes.LIST:
				arrlen = cur.execute("SELECT COUNT(*) FROM ds_arr WHERE ds_arr_id=?", (intval,)).fetchone()[0]
				write_dag_cbor_varint(stream, CborMajorType.ARRAY, arrlen)
				# TODO: grow stack if needed
				cur.execute("""
					UPDATE ds_stack
					SET ds_stack_key=NULL, ds_stack_val=ds_arr_val
					FROM (
						SELECT ds_arr_val, ROW_NUMBER()
							OVER (ORDER BY ds_arr_idx) AS ds_arr_sort_idx
						FROM ds_arr WHERE ds_arr_id=?
					)
					WHERE ds_stack_idx=?-ds_arr_sort_idx
				""", (intval, stack_ptr + arrlen + 1))
				stack_ptr += arrlen
			case DsTypes.MAP:
				maplen = cur.execute("SELECT COUNT(*) FROM ds_map WHERE ds_map_id=?", (intval,)).fetchone()[0]
				write_dag_cbor_varint(stream, CborMajorType.MAP, maplen)
				# TODO: grow stack if needed
				# XXX: this makes a copy of the map key. if it were long, it would be inefficient.
				# in practice however, long map keys ought to be rare.
				cur.execute("""
					UPDATE ds_stack
					SET ds_stack_key=ds_map_key, ds_stack_val=ds_map_val
					FROM (
						SELECT ds_map_key, ds_map_val, ROW_NUMBER()
							OVER (ORDER BY length(ds_map_key), ds_map_key) AS ds_map_sort_idx
						FROM ds_map WHERE ds_map_id=?
					)
					WHERE ds_stack_idx=?-ds_map_sort_idx
				""", (intval, stack_ptr + maplen + 1))
				stack_ptr += maplen

			case _:
				raise ValueError("unrecognised type")



if __name__ == "__main__":
	import os
	import json
	import io
	import dag_cbor # testing comparison

	DB_PATH = "../test.db"

	# gross: CD to where the script is
	os.chdir(os.path.dirname(os.path.abspath(__file__)))

	con = sqlite3.connect(DB_PATH)
	cur = con.cursor()
	
	for root, *_ in cur.execute("SELECT ds_root_obj FROM ds_root"):
		res = io.BytesIO()
		ds_to_dag_cbor_recursionless(res, cur, root)
		res = res.getvalue()
		print(res)
		print(json.dumps(dag_cbor.decode(res), indent="  "))