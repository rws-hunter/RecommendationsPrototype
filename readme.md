Prototype Related Items Processor
---

Process a SNS log file into a CSV file of product views:
`python process.py --input=data/jan_30_2022.log --out=views.csv --limit=1000000`

Process the created view CSV file into a CSV of item relations: 
`python model.py --input=views.csv --out=model.csv`
