import re
import argparse, sys
import xml.etree.ElementTree as ET

# Parse log messages from a file and yield the data from the line
def get_lines(file):
	# Break the file into lines
	lines = file.readlines()
	# Create a REGEX program to match log lines
	prog = re.compile(r'^\[.*\]:\s(.*)\n$')
	# For each line, if it matches, yield the data group
	for line in lines:
		matches = prog.match(line)
		if matches:
			yield matches.group(1)

# Extract the XML data from log messages
# NOTE: Essentially just builds a XML file from a list of log messages
def get_xml(lines):
	# Simple REGEX to check if a message is white-space-only
	prog = re.compile(r'^\s*$')
	# Create an empty XML-doc building string
	xml_doc = ""
	# For each line
	for line in lines:
		# Check if its an empty line
		if prog.match(line):
			# If it is, yield the doc and reset the XML string
			yield xml_doc
			xml_doc = ""
		else:
			# Append the string to the XML document
			xml_doc = xml_doc + line

# Parse the XML from a list of message
def parse_view(xml_list):
	# For each line in the XML list
	for xml in xml_list:
		# Try reading the xml data as a complete file
		try:
			root = ET.fromstring(xml)
		except:
			continue
		# Filter out non-page view events
		if root.tag != "profiling-event-visit-product-page":
			continue
		# Read out the Consumer ID and Item keys from the page view event XML
		cid = ""
		item = ""
		for child in root:
			if child.tag == "consumer-profile-id":
				cid = child.text
				continue
			if child.tag == "item":
				item = child.text
				continue
		# If both are present, yield the data as a CSV Row
		if cid != "" and item != "":
			yield f"{cid},{item}"

def main():
	# Parse command line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('--limit', help='Limit the number of items')
	args = parser.parse_args()
	# Read the limit argument
	limit = args.limit and int(args.limit) or None 
	# Open the log file
	with open("data/jan_30_2022.log", "r") as file:
		# Initalize a count to enforce limit and output result
		count = 0
		# Print the header row of the CSV
		print("consumer_id,item_key")
		# Parse each view from the XML file
		for view in parse_view(get_xml(get_lines(file))):
			# Output the view to the CSV
			print(view)
			# Enforce the limit constraint
			if limit and count > limit:
				break
			count = count + 1

if __name__ == '__main__':
	main()