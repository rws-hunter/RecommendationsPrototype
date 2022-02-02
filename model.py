import csv
import random
import argparse, sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Read in the view CSV file as an array of (user, item) pairs
def read_views(filename):
	views = []
	with open(filename, "r") as file:
		reader = csv.reader(file)
		# Skip the header row
		next(reader)
		# Read in the CSV as a list
		views = list(reader)
	return views

def main():
	# Parse command line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('--input', help='The input file to read')
	parser.add_argument('--out', help='The output file to write to')
	args = parser.parse_args()
	# Read the arguments
	filename_in = args.input
	filename_out = args.out
	# Load the views CSV file in the form [(user, item), ...]
	views = read_views(filename_in)
	# Get the set of users that have viewed each item
	item_to_user = defaultdict(set)
	for view in views:
		user = view[0]
		item = view[1]
		item_to_user[item].add(user)
	# Build a matrix of item to item view counts
	item_to_item = {}
	# Use a thread pool to make the O(n^2) collation process a bit faster
	with ThreadPoolExecutor(max_workers=12) as pool:
		# List of item keys for iteration purposes
		item_list = list(item_to_user.keys())
		n_items = len(item_list)
		# For each item
		def process_item(item_idx):
			# Get the item id
			item_id = item_list[item_idx]
			# Check all other items
			for j in range(item_idx+1, n_items):
				# Get the item id to check against
				item_id_next = item_list[j]
				# Get the set of users that have viewed both items
				# This is by getting the length of set intersection of the users
				shared_views = len(item_to_user[item_id_next] & item_to_user[item_id])
				# If that set is not empty, record the number of shared users
				if shared_views > 0:
					item_to_item[f"{item_id},{item_id_next}"] = shared_views
		# Enqueue a new job for each item and process in parallel
		for i in range(n_items-1):
			pool.submit(process_item, i)
		# Shutdown the job pool, blocking until all jobs are completed
		pool.shutdown(wait=True)
	# Open the output file
	with open(filename_out, "w") as file_out:
		# Output the data in the form item_a,item_b,shared_view_count
		# Sort the data highest-to-lowest views
		for key in dict(sorted(item_to_item.items(), key=lambda item: item[1], reverse=True)):
			count = item_to_item[key]
			file_out.write(f"{key},{count}\n")

if __name__ == '__main__':
	main()