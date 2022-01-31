import csv
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

def read_views(filename):
	views = []
	with open(filename, "r") as file:
		reader = csv.reader(file)
		next(reader)
		views = list(reader)
	return views

def main():
	# Load the views CSV file in the form [(user, item), ...]
	views = read_views("views.csv")
	# Get the set of users that have viewed each item
	item_to_user = defaultdict(set)
	for view in views:
		user = view[0]
		item = view[1]
		item_to_user[item].add(user)
	# Build a matrix of item to item view counts
	item_to_item = {}
	# Use a threadpool to make the O(n^2) collation process a bit faster
	with ThreadPoolExecutor(max_workers=10) as pool:
		# List of item keys for iteration purposes
		item_list = list(item_to_user.keys())
		n_items = len(item_list)
		# For each item
		def process_item(item_idx):
			item_id = item_list[item_idx]
			# Check all other items
			for j in range(item_idx+1, n_items):
				# Get the set of users that have viewed both items
				item_id_next = item_list[j]
				shared_views = item_to_user[item_id_next] & item_to_user[item_id]
				# If that set is not empty, record the number of shared users
				if len(shared_views) > 0:
					item_to_item[f"{item_id},{item_id_next}"] = len(shared_views)
		# Enqueue a new job for each item and process in parallel
		for i in range(n_items-1):
			pool.submit(process_item, i)
		pool.shutdown(wait=True)
	# Print out the data in the form item_a,item_b,shared_view_count
	# Sort the data highest-to-lowest views
	for key in dict(sorted(item_to_item.items(), key=lambda item: item[1], reverse=True)):
		count = item_to_item[key]
		print(f"{key},{count}")

if __name__ == '__main__':
	main()