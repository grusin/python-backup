#!/usr/bin/python

import sqlite3
import datetime
import os
import fnmatch
import hashlib

conn = conn = sqlite3.connect('db.sqlite', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
conn.row_factory = sqlite3.Row
conn.text_factory = str

#global variables
__str_xref_str2id = {} 
__str_xref_id2str = {}

def main():
	#create_database()

	folder_arr=['/mnt/md0/var/pybakup', ]
	
	xref_init()

	#create_snapshot("full", folder_arr)
	#show_db_stats()
	
	create_snapshot("diff", folder_arr)
	show_db_stats()

	#create_snapshot("diff")
        #show_db_stats()

	#create_snapshot("diff")
        #show_db_stats()

	print get_last_snapshot()	
	return

def generate_file_sha(file):
	m = hashlib.sha1()
	blocksize = m.block_size * 1024

	with open(file, "rb") as f:
		while True:
            		buf = f.read(blocksize)
            		if not buf:
               			break
           		m.update( buf )
    	
	return m.hexdigest()

def show_db_stats():
	c = conn.cursor()
	c.execute('''PRAGMA page_count''')
	page_count = c.fetchone()[0]

	c.execute('''PRAGMA page_size''')
	page_size =  c.fetchone()[0]

	c.execute('''select count(*) from str_xref''')
	str_xref_rows = c.fetchone()[0]

	c.execute('''select count(*) from file''')
        file_rows = c.fetchone()[0]

	c.execute('''select count(*) from snapshot''')
        snapshot_rows = c.fetchone()[0]


	print '--- DB STATS ---'
	print "DB size (KB): ", page_size * page_count / 1024 
	print 'str_xref rows: ', str_xref_rows
	print 'file rows: ', file_rows
	print 'snapshot rows: ', snapshot_rows
	print '----------------'
	return

def create_database():
	c = conn.cursor()

	c.execute('''
		CREATE TABLE str_xref (
			str_id INTEGER PRIMARY KEY AUTOINCREMENT
			,str_value text
		)''')

	c.execute('''
		CREATE TABLE file (
			snapshot_id integer
			,file_name_id integer
			,file_path_id interger
			,file_size integer
			,file_mtime datetime 
			,file_inode int
			,file_sha_id integer
			,file_status text
			,FOREIGN KEY(snapshot_id) REFERENCES snapshot(snapshot_id)
			,FOREIGN KEY(file_name_id) REFERENCES str_xref(str_id)
			,FOREIGN KEY(file_path_id) REFERENCES str_xref(str_id)
			,FOREIGN KEY(file_sha_id) REFERENCES str_xref(str_id)
	 	)''')


	c.execute('''
		CREATE table snapshot (
			snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT
			,snapshot_timestamp datetime
			,snapshot_type text
		)''')

	conn.commit()
	return

def xref_init():
	global __str_xref_id2str
	global __str_xref_str2id
	global __str_xref_sqlite_cursor
	
	__str_xref_sqlite_cursor = conn.cursor()

	__str_xref_sqlite_cursor.execute("select str_id, str_value from str_xref")

	for x in __str_xref_sqlite_cursor:
		id = x['str_id']
		str = x['str_value']
		__str_xref_id2str[id] = str
		__str_xref_str2id[str] = id

	return

def xref_add_xref(str):
	global __str_xref_id2str
        global __str_xref_str2id
	global __str_xref_sqlite_cursor

	__str_xref_sqlite_cursor.execute('''
		INSERT INTO str_xref 
		(str_value) VALUES (?)''', (str, ))
	
	id = __str_xref_sqlite_cursor.lastrowid	
	
	__str_xref_id2str[id] = str
	__str_xref_str2id[str] = id

	print 'adding str_xref: ', id, str

	return

def xref_str2id(str):
	if str == None:
		return None
	
	global __str_xref_str2id
	
	if str in __str_xref_str2id:
		return __str_xref_str2id[str]
	else:
		xref_add_xref(str)
		return __str_xref_str2id[str]
		 			
def xref_id2str(id):
	if id == None:
		return None		

	global __str_xref_id2str	
	return __str_xref_id2str[id] 	
	
def scan_all_folders(arr):
	file_list = []

	for d in arr:
		scan_folder(d, file_list)

	return file_list		


def scan_folder(root_dir, file_list):
        for root, _, files in os.walk(root_dir):
	        path = root.split('/')
                for file in files:
                        file_full_path = root + '/' + file
			file_stat = os.stat(file_full_path)

			obj = { } 
			obj['file_path'] = root
			obj['file_name'] = file
			obj['file_size'] = file_stat.st_size
			obj['file_inode'] = file_stat.st_ino
			obj['file_mtime'] = file_stat.st_mtime		
	
			file_list.append(obj)
	return
	
def get_last_snapshot():
        c = conn.cursor()
	
	c.execute('''
		select *
		from snapshot
		order by snapshot_timestamp desc
		limit 1''')

	res = c.fetchone()
	return res

def get_snapshot_files(snapshot_id):
	c = conn.cursor()
	c.execute('''select * from file where snapshot_id = ?''', (snapshot_id, ))
	d = {}
	for x in c.fetchall():
		k = str(x['file_path_id']) + ':' + str(x['file_name_id'])
		d[k] = x
	return d

def create_snapshot(snapshot_type, folder_arr):
	assert snapshot_type == "diff" or snapshot_type == "full"

	#insert snapshot
	current_snapshot_id = None
	last_snapshot_id = None

	if snapshot_type == "diff":		
		last_snapshot = get_last_snapshot()
		last_snapshot_id = last_snapshot['snapshot_id']
		assert last_snapshot != None

        c = conn.cursor()
        c.execute('''
        	INSERT INTO snapshot
                	(snapshot_timestamp, snapshot_type)
                        VALUES (?, ?)''', (datetime.datetime.now(), snapshot_type, ))
        
	current_snapshot_id = c.lastrowid

	file_list = scan_all_folders(folder_arr)

	#insert files
	for f in file_list:
		c.execute('''
			INSERT INTO file (
				snapshot_id
				,file_name_id
				,file_path_id
                        	,file_size
                        	,file_mtime
                        	,file_inode
			) VALUES (?, ?, ?, ?, ?, ?)''', ( 
			current_snapshot_id,
			xref_str2id(f['file_name']),
			xref_str2id(f['file_path']),
			f['file_size'],
			f['file_mtime'],
			f['file_inode'],
			)) 

	
	file_list = None

	#compare snapshots
	current_list = get_snapshot_files(current_snapshot_id)
	last_list = get_snapshot_files(last_snapshot_id)

	for cf_key, cf in current_list.iteritems():
		
		status = None
		sha_sum = None
		cf_file_path = xref_id2str(cf['file_path_id'])
		cf_file_name = xref_id2str(cf['file_name_id'])
		print cf_file_path
		print cf_file_name
		cf_file_full_name = os.path.join(cf_file_path, cf_file_name)

		print 'Processing file: ', cf_file_full_name
		if cf_key not in last_list:
			#not found
			status = 'new'
			sha_sum = generate_file_sha(cf_file_full_name)
		else:
			lf = last_list[cf_key]
			if lf['file_size'] == cf['file_size'] and lf['file_mtime'] == cf['file_mtime'] and lf['file_inode'] == cf['file_inode']:
				status = 'nc'
				sha_sum = xref_id2str(lf['file_sha_id'])
			else:
				status = 'mod'
				sha_sum = generate_file_sha(cf_file_full_name)
				if sha_sum == xref_id2str(lf['file_sha_id']):
					status = 'nc'
				
		print 'Status: ', status, '; sha_sum: ', sha_sum		
		
		c.execute('''UPDATE file
			SET file_sha_id = ?, file_status = ?
			WHERE snapshot_id = ? and file_name_id = ? and file_path_id = ?''', (
				xref_str2id(sha_sum)
				,status
				,current_snapshot_id
				,cf['file_name_id']
				,cf['file_path_id']
				)
			)

	conn.commit()
	return



main()
