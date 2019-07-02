def load_to_elastic(filename):
	"""
	Perform bulk upload to ElasticSearch in batch size of 400 records (200 recs in ND-JSON)
	"""
	batch=list()
	cnt=0
	print(filename)
	"""
	with open(filename,'r') as file_handler:
		for row in file_handler:
			cnt+=1
			batch.append(row)
			try:
				if len(batch)==400:
					helpers.bulk(es, index="comments_2008", doc_type="comment", actions=batch, stats_only=True)
					batch=[]
			except Exception as ex:
				print(ex)
				pass
	"""
	return

def elastic_search_load(data_dir, year, month):
	if month>0:
		directory = "{dir}/es_body_{yr}_{mn:02}/".format(dir=data_dir,yr=year,mn=month)
		files_list = [f for f in os.listdir(files_path) if f[:4] == "part"]
		# load each file
		for _file in files_list:
			load_to_elastic(year, _file, files_path)
	else:
		directory_list = [_dir for _dir in os.listdir(data_dir) if _dir[:2] == "es"]
		for _dir in directory_list:
			try:
				# try to load files in the dir
				files_path = "{0}/{1}/".format(data_dir,_dir)
				files_list = [f for f in os.listdir(files_path) if f[:4] == "part"]
				# load each file
				for _file in files_list:
				load_to_elastic(year, _file, files_path)
			except Exception as ex:
				print(ex)
				pass

if __name__ == '__main__':
	year = int(sys.argv[1])
	month = int(sys.argv[2])
	try:
		dir = "/home/ubuntu/spark-warehouse/Data/{0}".fomat(year)
		elastic_search_load(dir, year, month)
