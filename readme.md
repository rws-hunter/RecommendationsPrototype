Prototype Related Items Processor
---

Process a SNS log file into a CSV file of product views:
`python process.py --limit=1000000 > views.csv`

Process the view CSV file into a CSV of item relations: 
`python model.py > model.csv`
