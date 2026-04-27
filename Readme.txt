1. Reading the CSV
with open(input_path, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    columns = reader.fieldnames or []

csv.DictReader reads the CSV and automatically uses the first row (headers) as keys. Each subsequent row becomes a dictionary. So a CSV like this:

name, age, city
Alice, 30, Mumbai
Bob, 25, Pune
BECOMESSSSSSSSSSSSSSSSSSSS
[
  {"name": "Alice", "age": "30", "city": "Mumbai"},
  {"name": "Bob",   "age": "25", "city": "Pune"}
]

rows holds all those dicts, and columns holds just the header names.
2. Writing the JSON
json.dump takes the rows list and writes it into the output file. indent=2 makes it pretty-printed and human-readable. ensure_ascii=False makes sure special characters (like é, ñ, ₹) are written as-is instead of being escaped.

3. The output path logic
output_path = Path(output_path).resolve() if output_path else input_path.with_suffix(".json")
If you don't provide an output path, it just takes your input filename and swaps .csv with .json. So data.csv → data.json in the same folder.
