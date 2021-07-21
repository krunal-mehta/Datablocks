'''
Created By: 
	- Meet Shah    (202011047)
	- Krunal Mehta (202011051)

Last modified on: 11/04/2021
Title: Distributed Database Project
Topic: Data Summarization
'''


import psycopg2
import time
import re
import matplotlib.pyplot as plt

def open_connection():
	'''
	This function will open connection to database and will return connection object.
	'''

	print("------------------------------------------------------------\n")
	print("-> Connecting with PostgreSql.....")
	conn = psycopg2.connect(database="202011047_db", user = "postgres", password = "admin", host = "127.0.0.1", port = "5432")
	print("-> Opened database successfully.....")

	# Setting search_path
	cur = conn.cursor()
	cur.execute('''set search_path to "TPC"''')
	print("-> TPC-H schema selected.....")
	print("------------------------------------------------------------")

	return conn



def make_partition(conn):
	'''
	This function will divide lineitem table into lineitem_hot and lineitem_cold tables in following way.
	1. lineitem table sorted on l_shipdate,l_orderkey,l_partkey.
	2. selecting 200000 records from lineitem where l_shipdate starting from partition_start_date
	3. inserting first 65536 records into lineitem_cold.
	4. inserting remaining(200000-65536) records into lineitem_hot.  
	'''

	cur = conn.cursor()

	# Creating schema of hot and cold partition.
	print("\n-> Creating schema of hot and cold partition..... ")
	cur.execute(''' CREATE TABLE LINEITEM_HOT ( L_ORDERKEY    INTEGER NOT NULL,
	                             L_PARTKEY     INTEGER NOT NULL,
	                             L_SUPPKEY     INTEGER NOT NULL,
	                             L_LINENUMBER  INTEGER NOT NULL,
	                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
	                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
	                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
	                             L_TAX         DECIMAL(15,2) NOT NULL,
	                             L_RETURNFLAG  CHAR(1) NOT NULL,
	                             L_LINESTATUS  CHAR(1) NOT NULL,
	                             L_SHIPDATE    DATE NOT NULL,
	                             L_COMMITDATE  DATE NOT NULL,
	                             L_RECEIPTDATE DATE NOT NULL,
	                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
	                             L_SHIPMODE     CHAR(10) NOT NULL,
	                             L_COMMENT      VARCHAR(44) NOT NULL,
	                             L_EXTRA VARCHAR(25)); ''')

	cur.execute(''' CREATE TABLE LINEITEM_COLD ( L_ORDERKEY    INTEGER NOT NULL,
	                             L_PARTKEY     INTEGER NOT NULL,
	                             L_SUPPKEY     INTEGER NOT NULL,
	                             L_LINENUMBER  INTEGER NOT NULL,
	                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
	                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
	                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
	                             L_TAX         DECIMAL(15,2) NOT NULL,
	                             L_RETURNFLAG  CHAR(1) NOT NULL,
	                             L_LINESTATUS  CHAR(1) NOT NULL,
	                             L_SHIPDATE    DATE NOT NULL,
	                             L_COMMITDATE  DATE NOT NULL,
	                             L_RECEIPTDATE DATE NOT NULL,
	                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
	                             L_SHIPMODE     CHAR(10) NOT NULL,
	                             L_COMMENT      VARCHAR(44) NOT NULL,
	                             L_EXTRA VARCHAR(25)); ''')

	print("-> Schema created successfully for LINEITEM_COLD and LINEITEM_HOT tables.....")
	
	# Creating schema of linitem_sample table.
	# print("\n-> Creating schema of linitem_sample table..... ")
	cur.execute(''' CREATE TABLE LINEITEM_SAMPLE ( L_ORDERKEY    INTEGER NOT NULL,
	                             L_PARTKEY     INTEGER NOT NULL,
	                             L_SUPPKEY     INTEGER NOT NULL,
	                             L_LINENUMBER  INTEGER NOT NULL,
	                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
	                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
	                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
	                             L_TAX         DECIMAL(15,2) NOT NULL,
	                             L_RETURNFLAG  CHAR(1) NOT NULL,
	                             L_LINESTATUS  CHAR(1) NOT NULL,
	                             L_SHIPDATE    DATE NOT NULL,
	                             L_COMMITDATE  DATE NOT NULL,
	                             L_RECEIPTDATE DATE NOT NULL,
	                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
	                             L_SHIPMODE     CHAR(10) NOT NULL,
	                             L_COMMENT      VARCHAR(44) NOT NULL,
	                             L_EXTRA VARCHAR(25)); ''')

	# Intializing data structure varibles
	global min_cold
	global max_cold
	global min_hot
	global max_hot
	global partition_start_date

	# Insert into LINEITEM_COLD and LINEITEM_HOT tables.
	print("-> Inserting data into LINEITEM_COLD, LINEITEM_HOT tables from LINEITEM table.....")

	cur.execute(''' INSERT INTO LINEITEM_COLD ( 
		                        SELECT * FROM LINEITEM 
		            			WHERE L_SHIPDATE >= date '''+ str(partition_start_date) +''' ORDER BY L_SHIPDATE, L_ORDERKEY, L_PARTKEY 
		            			LIMIT '''+ str(max_cold-min_cold) +''' OFFSET '''+ str(min_cold) +
		            			'''); ''')

	cur.execute(''' INSERT INTO LINEITEM_HOT ( 
		 						SELECT * FROM LINEITEM 
		 						WHERE L_SHIPDATE >= date '''+ str(partition_start_date) +''' ORDER BY L_SHIPDATE, L_ORDERKEY, L_PARTKEY 
		 						LIMIT '''+ str(max_hot-min_hot) +''' OFFSET '''+ str(min_hot) +
		 						'''); ''')
								
	cur.execute(''' INSERT INTO LINEITEM_SAMPLE ( 
		 						SELECT * FROM LINEITEM 
		 						WHERE L_SHIPDATE >= date '''+ str(partition_start_date) +''' ORDER BY L_SHIPDATE, L_ORDERKEY, L_PARTKEY 
		 						LIMIT '''+ str(max_hot) +'''); ''')								

	print("-> Inserted data successfully.....")

	conn.commit()
	print("------------------------------------------------------------")



def compress_colddata_on_lshipdate(conn):
	'''
	This function will 
	1. Extract min and max of l_shipdate from lineitem_cold
	2. Calculate and store delta value of l_shipdate for all tuples by subtracting min value from it in a new column.
	3. Drop the column containing original value of l_shipdate.
	4. Rename the new column containing compressed value to l_shipdate.
	'''

	global min_date
	global max_date

	cur = conn.cursor()
	print("\n-> Extracting min and max of l_shipdate from lineitem_cold.....")

	# Extracting min_lshipdate from lineitem_cold table
	cur.execute(''' SELECT L_SHIPDATE FROM LINEITEM_COLD LIMIT 1; ''')
	res = cur.fetchall()
	for r in res:
		min_date = str(r[0])
	print("-> Cold Block L_shipdate Min: {}".format(min_date))

	# Extracting max_lshipdate from lineitem_cold table
	cur.execute(''' SELECT L_SHIPDATE FROM LINEITEM_COLD ORDER BY L_SHIPDATE DESC LIMIT 1; ''')
	res = cur.fetchall()
	for r in res:
		max_date = str(r[0])
	print("-> Cold Block L_shipdate Max: {}".format(max_date))

	print("-> Compressing lineitem_cold table by relacing l_shipdate with delta value of l_shipdate.....")
	# Adding new column containing delta value of l_shipdate
	cur.execute(''' ALTER TABLE LINEITEM_COLD ADD COLUMN l_shipdate_delta INTEGER; ''')
	conn.commit()

	# Inserting delta values to new column
	cur.execute(" UPDATE LINEITEM_COLD SET L_SHIPDATE_DELTA = L_SHIPDATE - DATE '"+ min_date +"' WHERE L_SHIPDATE IN (SELECT L_SHIPDATE FROM LINEITEM_COLD);")
	conn.commit()

	# Deleting old column
	cur.execute(''' ALTER TABLE LINEITEM_COLD DROP COLUMN L_SHIPDATE; ''')
	conn.commit()

	# Renaming new column to l_shipdate
	cur.execute(''' ALTER TABLE LINEITEM_COLD RENAME COLUMN L_SHIPDATE_DELTA TO L_SHIPDATE; ''')
	conn.commit()

	print("-> Successfully compressed lineitem_cold table.....")
	print("------------------------------------------------------------")



def make_lookup_table(conn):
	'''
	This function will compute lookup table by,
	1. Find number of entry in lookup table by subtracting min_date from max_date
	2. It will set limit of each entry by calculating number of tuples present in lineitem_cold table
	3. It will set offset of each entry by calculating number of records of all previous entry.

	Lookup table structure example,
	{
	0 : [0(offset),10(limit)],
	1 : [10,20]
	2 : [20,35]
	...
	}
	'''

	global min_date
	global max_date
	global look_up_table

	print("\n-> Creating Look up Table.....")
	cur = conn.cursor()
	cur.execute("SELECT DATE '"+ max_date +"' - DATE '"+ min_date +"';")
	res = cur.fetchall()
	for r in res:
		no_of_entry = r[0]

	print("-> Total number of entries in look up table: {}".format(no_of_entry+1))

	print("\n********************* Look up table *********************")
	print("\n%10s \t| %10s \t| %10s \t| %15s"%('Index','Offset','Limit','No_of_records'))
	start_position = 0
	for i in range(no_of_entry+1):
		look_up_table[i] = [start_position]
		cur.execute("SELECT COUNT(*) FROM LINEITEM_COLD WHERE L_SHIPDATE = "+ str(i) +";")
		res = cur.fetchall()
		for r in res:
			limit = int(r[0])
		look_up_table[i].append(limit)
		print("%10s \t| %10s \t| %10s\t| %15s"%(i,start_position,start_position+limit,limit))
		start_position += limit
	print("\n*********************************************************")

	print(" \n-> Successfully created Lookup table.....")
	print("------------------------------------------------------------")



def original_query_execution(conn,query,print_flag):
	'''
	This function will execute original query and return query execution time.
	'''
	cur = conn.cursor()
	starting_time = time.time()

	# query result will be in ans.
	ans = []
	if query.count("LINEITEM L") > 0:
		query = query.replace("LINEITEM","(SELECT * FROM LINEITEM_SAMPLE ORDER BY L_SHIPDATE, L_ORDERKEY, L_PARTKEY)")
	else :
		query = query.replace("LINEITEM","(SELECT * FROM LINEITEM_SAMPLE ORDER BY L_SHIPDATE, L_ORDERKEY, L_PARTKEY) AS LINEITEM")
	cur.execute(query)
	res = cur.fetchall()
	for r in res:
		ans.append(r)

	ending_time = time.time()
	qet = ending_time-starting_time
	print("-> Query executed successfully.....")
	print("-> Executed query in {} second time.".format(qet))

	# Printing ans
	if print_flag==True:
		print("\n*************** Query Output(Full Scan) ***************\n")
		for i in ans:
			print(i)
		print("\n********************************************")
	print("------------------------------------------------------------")
	return qet


def check_block(conn,query):
	'''
	This function will decide whether query will be executed on lineitem_cold,lineitem_hot or both.
	If whole lineitem_cold table cannot be skipped then, potential scan range will be extracted from lookup table.
	Function will return list containing information about potential scan range, 
	and query will be executed only on those tuples.

	Possible return format:
	[]
	[['LINEITEM_HOT',0,0]]
	[['LINEITEM_COLD',OFFSET,LIMIT],['LINEITEM_HOT',0,0]]
	'''


	global look_up_table
	global min_date_compressed
	global max_date_compressed
	global min_cold
	global max_cold
	global min_hot
	global max_hot

	print("\n")

	# If query doesn't contain table lineitem, then it will not execute on any of them.
	if query.count('LINEITEM')==0:
		print("-> Query not contain lineitem table.....")
		print("-> Number of tuples accessed in LINEITEM_COLD: 0")
		print("-> Number of tuples accessed in LINEITEM_HOT: 0")
		print("------------------------------------------------------------")
		return []


	# If query conatain linitem table and doesn't contain where or l_shipdate attribute after where, 
	# then it will execute query on whole lineitem_cold and lineitem_hot.
	if query.count('WHERE')==0 or query.count('L_SHIPDATE',query.count('WHERE'))==0:
		print("-> Query contain lineitem table but not contain either 'where' or l_shipdate attribute after 'where'.....")
		print("-> Number of tuples accessed in LINEITEM_COLD: {} [Offset: {}]".format(look_up_table[max_date_compressed][0]+look_up_table[max_date_compressed][1],look_up_table[min_date_compressed][0]))
		print("-> Number of tuples accessed in LINEITEM_HOT: {}".format(max_hot-min_hot))
		print("------------------------------------------------------------")
		return [['LINEITEM_COLD',look_up_table[min_date_compressed][0],look_up_table[max_date_compressed][0]+look_up_table[max_date_compressed][1]],['LINEITEM_HOT',0,0]]
	
	# If query contain l_shipdate attribute after where, then 'extact_dates' function will call, 
	# and list containing conditions associated with l_shipdates are extracted from query. 
	date_list = extract_dates(conn,query)

	if date_list == []:
		print("-> Query not having specific date in l_shipdate condition.....")
		print("-> Number of tuples accessed in LINEITEM_COLD: {} [Offset: {}]".format(look_up_table[max_date_compressed][0]+look_up_table[max_date_compressed][1],look_up_table[min_date_compressed][0]))
		print("-> Number of tuples accessed in LINEITEM_HOT: {}".format(max_hot-min_hot))
		print("------------------------------------------------------------")
		return [['LINEITEM_COLD',look_up_table[min_date_compressed][0],look_up_table[max_date_compressed][0]+look_up_table[max_date_compressed][1]],['LINEITEM_HOT',0,0]]

	
	# temp_min and temp_max will store lowest and highest key value of lookup table to be accessed.
	temp_min = False
	temp_max = False

	# date_list will contain operator along with date. i.e, [['>=',delta('1994-01-01')],['<',delta('1995-01-01')]]
	
	i = date_list[0]
	i[1] = int(i[1])

	# For each operator, we will find appropiate temp_min and temp_max.

	if i[0]=="<=":
		if min_date_compressed>i[1]:
			less_e_flag=[i[1]]
		elif max_date_compressed<=i[1]:
			temp_min = min_date_compressed
			temp_max = max_date_compressed
		else:
			temp_min = min_date_compressed
			temp_max = i[1] 
	if i[0]=="<":
		if min_date_compressed>=i[1]:
			less_flag=[i[1]]
		elif max_date_compressed<i[1]:
			temp_min = min_date_compressed
			temp_max = max_date_compressed
		elif max_date_compressed==i[1]:
			temp_min = min_date_compressed
			temp_max = i[1]-1
		else:
			temp_min = min_date_compressed
			temp_max = i[1]-1
	if i[0]==">=":
		if max_date_compressed<i[1]:
			greater_e_flag=[i[1]]
		elif min_date_compressed>=i[1]:
			temp_min = min_date_compressed
			temp_max = max_date_compressed
		else:
			temp_min = i[1]
			temp_max = max_date_compressed 
	if i[0]==">":
		if max_date_compressed<=i[1]:
			greater_flag=[i[1]]
		elif min_date_compressed>i[1]:
			temp_min = min_date_compressed
			temp_max = max_date_compressed
		elif min_date_compressed==i[1]:
			temp_min = i[1]+1
			temp_max = max_date_compressed
		else:
			temp_min = i[1]+1
			temp_max = max_date_compressed
	if i[0]=="=":
		if min_date_compressed <= i[1] and i[1] <= max_date_compressed:
			temp_min = i[1]
			temp_max = i[1]

	# If we have multiple condition, 
	# then after first iteration, we are evaluating second condition only on found temp_min and temp_max. 

	if len(date_list)>1:
		i = date_list[1]
		i[1] = int(i[1])
		if temp_min==False and temp_max==False:
			pass
		else:
			if i[0]=="<=":
				if temp_min>i[1]:
					temp_min = False
					temp_max = False
				elif temp_max<=i[1]:
					temp_min = temp_min
					temp_max = temp_max
				else:
					temp_min = temp_min
					temp_max = i[1] 
			if i[0]=="<":
				if temp_min>=i[1]:
					temp_min = False
					temp_max = False
				elif temp_max<i[1]:
					temp_min = temp_min
					temp_max = temp_max
				elif max_date_compressed==i[1]:
					temp_min = temp_min
					temp_max = i[1]-1
				else:
					temp_min = temp_min
					temp_max = i[1]-1 
			if i[0]==">=":
				if temp_max<i[1]:
					temp_min = False
					temp_max = False
				elif temp_min>=i[1]:
					temp_min = temp_min
					temp_max = temp_max
				else:
					temp_min = i[1]
					temp_max = temp_max
			if i[0]==">":
				if temp_max<=i[1]:
					temp_min = False
					temp_max = False
				elif temp_min>i[1]:
					temp_min = temp_min
					temp_max = temp_max
				elif temp_min==i[1]:
					temp_min = i[1]+1
					temp_max = temp_max
				else:
					temp_min = i[1]+1
					temp_max = temp_max
			if i[0]=="=":
				if temp_min <= i[1] and i[1] <= temp_max:
					temp_min = i[1]
					temp_max = i[1]	
				else:
					temp_min = False
					temp_max = False


	# If none of the conditions satisfy then whole cold block will be skipped.
	if temp_min==False and temp_max==False:
		print("-> Whole LINEITEM_COLD will be skipped.....")
		print("-> Number of tuples accessed in LINEITEM_COLD: 0") 
		print("-> Number of tuples accessed in LINEITEM_HOT: {}".format(max_hot-min_hot))
		print("------------------------------------------------------------")
		return [['LINEITEM_HOT',0,0]]

	# Otherwise scan will be perfrmed only on range found from lookup table by temp_min and temp_max.
	else:
		print("-> LINEITEM_COLD will be scanned.....")
		print("-> Lookup Table key interval: {} to {}".format(temp_min,temp_max))
		print("-> Number of tuples accessed in LINEITEM_COLD: {} [Offset: {}]".format(look_up_table[temp_max][0]+look_up_table[temp_max][1]-look_up_table[temp_min][0],look_up_table[temp_min][0]))
		print("-> Number of tuples accessed in LINEITEM_HOT: {}".format(max_hot-min_hot))
		print("------------------------------------------------------------")
		return [['LINEITEM_COLD',look_up_table[temp_min][0],look_up_table[temp_max][0]+look_up_table[temp_max][1]-look_up_table[temp_min][0]],['LINEITEM_HOT',0,0]]




def extract_dates(conn,query):
	'''
	This function will return list containing conditions associated with l_shipdates, extracted from query.
	Possible return format:
	Q1.  [['<=',delta('1998-08-15')]]
	Q3.  [['>',delta('1995-03-13')]]
	Q6.  [['>=',delta('1994-01-01')],['<',delta('1995-01-01')]]
	Q7.  [['>=',delta('1995-01-01')],['<=',delta('1996-12-31')]]
	Q14. [['>=',delta('1996-12-01')],['<',delta('1997-01-01')]]
	'''

	global min_date

	# end_list is a list of possible words, that may arrive after l_shipdate condition.
	end_list = ['GROUP','ORDER','AND','LIMIT',')',';']

	# Finding all indexes of 'l_shipdate' in query
	res = [i.start() for i in re.finditer('L_SHIPDATE', query)]

	# p_list will contain extracted l_shipdate conition.
	# i.e, ['l_shipdate <= date '1998-12-01' - interval '108' day']
	p_list = []

	for i in res:
		# if 'l_shipdate' index is after 'where' index 
		if query.index('WHERE') < i:

			# temp_s is query after 'l_shipdate' index
			temp_s = query[i:]

			# flag will True if 'between' is there after immediately 'l_shipdate' index 
			flag = False
			try:
				if temp_s.index('BETWEEN',0,25)>0:
					flag = True
			except:
				pass

			min_index = len(temp_s)

			# Iterate through all words in end_list.
			for j in end_list:
				# If 'between' used, skip first 'and'.
				if j=='AND' and flag == True:
					t  = temp_s.index(j)
					continue
				# Find index of 'j' word, and set to min_index if it is lower than min_index. 
				try:
					q = temp_s.index(j)
					if q<min_index:
						min_index=q
				except:
					pass

			# For case of 'between', we will serach next 'and' index and set to min_index if it is lower than min_index. 
			if flag==True:
				try:
					q = temp_s.index('AND',t+3)
					if q+t<min_index:
						min_index=q+t
				except:
					pass			
			p_list.append(temp_s[:min_index])

	print("-> Extracted l_shipdate condition: ")
	for i in p_list:
		print(i)

	# Store ans in date_list.
	# i.e, p_list ['l_shipdate <= date '1998-12-01' - interval '108' day'] will be conveted to [['<=',delta('1998-08-15')]]
	date_list = []

	cur = conn.cursor()

	for i in p_list:
		temp_ans = []
		temp_list = i.split()

		# Will return [] in case of 'l_shipdate < l_commitdate' as we are not having specific date in condition.
		try:
			x = temp_list.index('DATE')
		except:
			continue

		# To handle 'between' case.
		# i.e, ["l_shipdate between date '1995-01-01' and date '1996-12-31'"] will return [['>=',delta('1995-01-01')],['<=',delta('1996-12-31')]]. 
		if temp_list[1]=='BETWEEN':
			and_index = temp_list.index('AND')
			temp_query = "SELECT "+' '.join(temp_list[2:and_index ])+" - DATE '"+min_date+"';"
			temp_ans.append('>=')
			cur.execute(temp_query)
			res = cur.fetchall()
			for r in res:
				temp_ans.append(str(r[0]).split()[0])
			date_list.append(temp_ans)
			temp_ans = []
			temp_query = "SELECT "+' '.join(temp_list[and_index+1: ])+" - DATE '"+min_date+"';"
			temp_ans.append('<=')
			cur.execute(temp_query)
			res = cur.fetchall()
			for r in res:
				temp_ans.append(str(r[0]).split()[0])

		# In normal case.
		# i.e, ["l_shipdate > date '1995-03-13'"] will return [['>',delta('1995-03-13')]].
		else:
			temp_ans.append(temp_list[1])
			temp_query = "SELECT "+' '.join(temp_list[2: ])+" - DATE '"+min_date+"';"
			cur.execute(temp_query)
			res = cur.fetchall()
			for r in res:
				temp_ans.append(str(r[0]).split()[0])
		date_list.append(temp_ans)

	print("\n-> Converted l_shipdate condition in the following way: ")
	print(date_list)
	print("\n")

	return date_list




def run_query(conn,query,l,print_flag):
	'''
	This function will execute query on partitions and range provided by check_block function.
	If we have to access cold_data table, first we uncompress cold data of given range into temporary storage
	and will evaluate query on that temporary storage only.
	After evaluating, temporary storage will be deleted.
	This function will return compressed query execution time.
	'''
	global min_date

	cur = conn.cursor()
	starting_time = time.time()

	# query result will be in ans.
	ans = []
	cold_flag = False
	
	# normal query execution in the case of no lineitem table accessed.
	if l==[]:
		cur.execute(query)
		res = cur.fetchall()
		for r in res:
			ans.append(r)
	else:
		for i in l:
			# If we need to access linitem_cold table
			if i[0]=='LINEITEM_COLD':
				cur.execute(''' CREATE TABLE LINEITEM_TEMP_UNCOMPRESS ( L_ORDERKEY    INTEGER NOT NULL,
				                             L_PARTKEY     INTEGER NOT NULL,
				                             L_SUPPKEY     INTEGER NOT NULL,
				                             L_LINENUMBER  INTEGER NOT NULL,
				                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
				                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
				                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
				                             L_TAX         DECIMAL(15,2) NOT NULL,
				                             L_RETURNFLAG  CHAR(1) NOT NULL,
				                             L_LINESTATUS  CHAR(1) NOT NULL,
				                             L_SHIPDATE    DATE NOT NULL,
				                             L_COMMITDATE  DATE NOT NULL,
				                             L_RECEIPTDATE DATE NOT NULL,
				                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
				                             L_SHIPMODE     CHAR(10) NOT NULL,
				                             L_COMMENT      VARCHAR(44) NOT NULL,
				                             L_EXTRA VARCHAR(25)); ''')

				print("-> Uncompressing LINEITEM_COLD table for matching range in temporary storage.....")
				cur.execute(" INSERT INTO LINEITEM_TEMP_UNCOMPRESS (SELECT L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY, L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,DATE '"+ min_date +"' + L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT,L_EXTRA  FROM LINEITEM_COLD ORDER BY L_SHIPDATE, L_ORDERKEY, L_PARTKEY LIMIT "+str(i[2])+" OFFSET "+str(i[1])+");")
				conn.commit()
				print("-> LINEITEM_COLD table uncompressed successfully.....")
				cold_flag = True

			# Access linitem_hot table 
			if i[0]=='LINEITEM_HOT':
				if cold_flag==False:
					# If linitem_cold skipped, executing query only on linitem_hot table 
					query_hot = query.replace('LINEITEM','LINEITEM_HOT')
					cur.execute(query_hot)
					res = cur.fetchall()
					for r in res:
						ans.append(r)
				else:
					# Executing query on union of linitem_hot table and temporary uncompressed table
					if query.count("LINEITEM L") > 0:
						query_combine = query.replace('LINEITEM','(SELECT * FROM LINEITEM_HOT UNION SELECT * FROM LINEITEM_TEMP_UNCOMPRESS) ')
					else :
						query_combine = query.replace('LINEITEM','(SELECT * FROM LINEITEM_HOT UNION SELECT * FROM LINEITEM_TEMP_UNCOMPRESS) AS LINEITEM')
					cur.execute(query_combine)
					res = cur.fetchall()
					for r in res:
						ans.append(r)

	ending_time = time.time()
	qet = ending_time-starting_time
	print("-> Query executed successfully.....")
	print("-> Executed query in {} second time.".format(qet))

	# Temporary storage table deleted if created
	if cold_flag==True:
		cur.execute("DROP TABLE LINEITEM_TEMP_UNCOMPRESS;")
		conn.commit()
		print("-> Temporary storage table deleted.....")


	# Printing ans
	if print_flag==True:
		print("\n*************** Query Output(Datablock Scan) ***************\n")
		for i in ans:
			print(i)
		print("\n************************************************************")
	print("------------------------------------------------------------")

	return qet


def close_connection(conn):
	'''
	This function will drop extra tables created for the process and close the database connection with PosgreSql.
	'''

	cur = conn.cursor()
	cur.execute("DROP TABLE LINEITEM_COLD;")
	cur.execute("DROP TABLE LINEITEM_HOT;")
	cur.execute("DROP TABLE LINEITEM_SAMPLE;")
	conn.commit()
	conn.close()
	print("\n -> Dropped tables created during process.....")
	print("-> Closed database successfully.....")
	print("------------------------------------------------------------")






# ===================== Main =====================

AET_starting_time = time.time()

# Initializing global variables 
min_date = 0
max_date = 0
look_up_table = {}
min_cold = 0
max_cold = 65536
min_hot = 65536
max_hot = 200000
partition_start_date = "'1992-01-02'"
# Opening PostgreSql connection
conn = open_connection()

# Dividing lineitem table into hot and cold partition
make_partition(conn)

# Compressing cold data
compress_colddata_on_lshipdate(conn)

# Computing lookup table
make_lookup_table(conn)

# Finding and storing compressed value of min and max of l_shipdate 
look_up_table_keys = list(look_up_table.keys())
look_up_table_keys.sort()
min_date_compressed = look_up_table_keys[0]
max_date_compressed = look_up_table_keys[-1]

AET_ending_time = time.time()
AET = AET_ending_time - AET_starting_time
print("\n Algorithm Execution Time : {} sec.....".format(round(AET,2)))

while(1):
	choice = input("\nDo you want to execute query?[y/n]: ")

	if choice=='y' or choice=='Y':

		query = input("\nEnter Query: \n")
		query = query.upper()


		# Deciding on which partitions scan will perform
		l = check_block(conn,query)

		
		# Executing query
		
		cache = input("Please press enter after clearing cache.....")
		
		print("\n-> Starting query execution(Full Scan-Cold)......")
		normal_qet_cold_scan = original_query_execution(conn,query,False)
		print("\n-> Starting query execution(Full Scan-Hot[1])......")
		normal_qet_hot1_scan = original_query_execution(conn,query,False)
		print("\n-> Starting query execution(Full Scan-Hot[2])......")
		normal_qet_hot2_scan = original_query_execution(conn,query,False)
		print("\n-> Starting query execution(Full Scan-Hot[3])......")
		normal_qet_hot3_scan = original_query_execution(conn,query,True)
		
		cache = input("Please press enter after clearing cache.....")
		
		print("\n-> Starting query execution(Datablock Scan-Cold)......")
		compress_qet_cold_scan = run_query(conn,query,l,False)
		print("\n-> Starting query execution(Datablock Scan-Hot[1])......")
		compress_qet_hot1_scan = run_query(conn,query,l,False)
		print("\n-> Starting query execution(Datablock Scan-Hot[2])......")
		compress_qet_hot2_scan = run_query(conn,query,l,False)
		print("\n-> Starting query execution(Datablock Scan-Hot[3])......")
		compress_qet_hot3_scan = run_query(conn,query,l,True)

		# Graph plot
		fig, ax = plt.subplots(1, 1)

		labels = ['Full Scan','Datablock Scan']
		cold = [normal_qet_cold_scan,compress_qet_cold_scan]
		hot1 = [normal_qet_hot1_scan,compress_qet_hot1_scan]
		hot2 = [normal_qet_hot2_scan,compress_qet_hot2_scan]
		hot3 = [normal_qet_hot3_scan,compress_qet_hot3_scan]


		ax.bar(labels[0],cold[0],label='Cold Scan',color=(0,0.5,0.9))
		ax.bar(labels[0],hot1[0],label='Hot Scan 1',color=(1, 0, 0),bottom=cold[0])
		ax.bar(labels[0],hot2[0],label='Hot Scan 2',color=(1,0.6,0.4),bottom=hot1[0]+cold[0])
		ax.bar(labels[0],hot3[0],label='Hot Scan 3',color=(1,0.4,0.4),bottom=hot2[0]+hot1[0]+cold[0])

		ax.bar(labels[1],cold[1],color=(0,0.5,0.9))
		ax.bar(labels[1],hot1[1],color=(1, 0, 0),bottom=cold[1])
		ax.bar(labels[1],hot2[1],color=(1,0.6,0.4),bottom=hot1[1]+cold[1])
		ax.bar(labels[1],hot3[1],color=(1,0.4,0.4),bottom=hot2[1]+hot1[1]+cold[1])

		for i, v in enumerate(cold):
			ax.text(i,v-(v/2),str(round(v,2)),ha='center', fontweight='bold',fontsize=10)
		for i, v in enumerate(hot1):
			ax.text(i,v-(v/2)+cold[i],str(round(v,2)),ha='center', fontweight='bold',fontsize=10)
		for i, v in enumerate(hot2):
			ax.text(i,v-(v/2)+cold[i]+hot1[i],str(round(v,2)),ha='center', fontweight='bold',fontsize=10)
		for i, v in enumerate(hot3):
			ax.text(i,v-(v/2)+cold[i]+hot1[i]+hot2[i],str(round(v,2)),ha='center', fontweight='bold',fontsize=10)

		ax.set_ylabel('Query Execution Time (in seconds)')
		ax.set_xlabel('Data Summarization Technique')

		ax.legend()
		plt.show() 



	elif choice=='n' or choice=='N':
		print("\nExiting from the program.....\n")
		break

	else:
		print("\nEnter valid input!!!")
		continue


# Closing PostgreSql connection
close_connection(conn)
