import psycopg2
import yaml
import spacy
import re
import pandas as pd
from bs4 import BeautifulSoup
import time
from xlsxwriter import Workbook

# Load configuration from YAML file
with open('../python/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Database connection parameters
db_config = config['local_db']  # Adjust to 'remote_db' if needed

# Connect to the database
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    dbname=db_config['database'],
    user=db_config['user'],
    password=db_config['password']
)

# Define query parameters
query_limit = config.get('query_spacy_limit', 2500000)
offset = config.get('offset', 0)
query_text = 'gold mine'

# SQL query to retrieve stories containing the keyword "gold mine"
story_query = f"SELECT id, story FROM public.\"DJ_NEWS_STORIES\" WHERE story LIKE '%{query_text}%' LIMIT {query_limit} OFFSET {offset};"

# Load spaCy's English model for NER
nlp = spacy.load("en_core_web_sm")

# Function to clean text: removes URLs, HTML tags, punctuation, numbers, and extra whitespace
def clean_text(text):
    text = re.sub(r'https?://\S+|www\.\S+', '', text)  # Remove URLs
    text = BeautifulSoup(text, "html.parser").get_text()  # Remove HTML tags
    text = re.sub(r'[^a-zA-Z\s]', '', text)  # Keep only letters and spaces
    return re.sub(r'\s+', ' ', text).strip()  # Remove extra whitespace

# Start processing
start_time = time.time()

# Execute query to retrieve stories
cursor = conn.cursor()
cursor.execute(story_query)
result = cursor.fetchall()

# Collect entities and counts
unique_entities = set()
type_counts = {}
total_count = 0

# Process each story in the results
for row in result:
    text = clean_text(row[1])  # Clean the story text
    doc = nlp(text)  # Process cleaned text with spaCy NER

    # Extract and store relevant entities
    for ent in doc.ents:
        if ent.label_ in ['ORG', 'GPE', 'PERSON']:
            unique_entities.add((ent.text.strip(), ent.label_))  # Add to unique entities
            type_counts[ent.label_] = type_counts.get(ent.label_, 0) + 1
            total_count += 1

# Prepare Excel file with multiple sheets
workbook = Workbook("recognized_entities_spacy.xlsx")

# Sheet 1: Unique Entities
unique_entities_sheet = workbook.add_worksheet("Unique Entities")
unique_entities_sheet.write_row(0, 0, ["Entity", "Tag"])
for idx, (text, tag) in enumerate(unique_entities, start=1):
    unique_entities_sheet.write_row(idx, 0, [text, tag])

# Sheet 2: Summary
summary_sheet = workbook.add_worksheet("Summary")
summary_sheet.write_row(0, 0, ["Tag", "Count"])
for idx, (tag, count) in enumerate(type_counts.items(), start=1):
    summary_sheet.write_row(idx, 0, [tag, count])
summary_sheet.write_row(len(type_counts) + 1, 0, ["TOTAL", total_count])

# Close the workbook and database connection
workbook.close()
cursor.close()
conn.close()

# Print execution time
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
