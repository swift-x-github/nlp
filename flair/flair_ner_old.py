#!pip install flair
import psycopg2
import yaml
import pandas as pd
import re
from bs4 import BeautifulSoup
import time
from flair.data import Sentence
from flair.models import SequenceTagger

# Load configuration from YAML file
with open('config.yaml', 'r') as file:
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
query_limit = config.get('query_flair_limit', 2500000)
offset = config.get('offset', 0)
query_text = 'gold mine'

# SQL query to retrieve stories containing the keyword "gold mine"
story_query = f"SELECT id, story FROM public.\"DJ_NEWS_STORIES\" WHERE story LIKE '%{query_text}%' LIMIT {query_limit} OFFSET {offset};"

# Load Flair's pre-trained NER model
tagger = SequenceTagger.load("ner")

# Function to clean text: removes URLs, HTML tags, punctuation, numbers, and extra whitespace
def clean_text(text):
    # Remove URLs
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    
    # Remove HTML tags
    text = BeautifulSoup(text, "html.parser").get_text()
    
    # Remove punctuation, digits, and non-printable characters
    text = re.sub(r'[^a-zA-Z\s]', '', text)  # Keep only letters and spaces
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra whitespace

    return text

# Start processing
start_time = time.time()

# Execute query to retrieve stories
cursor = conn.cursor()
cursor.execute(story_query)
result = cursor.fetchall()

# Collect entities and counts
unique_entities_set = set()
all_entities = set()
type_counts = {}
total_count = 0

# Process each story in the results
for row in result:
    story_id = row[0]
    text = row[1]
    
    # Clean the story text
    cleaned_text = clean_text(text)
    
    # Process cleaned text with Flair NER
    sentence = Sentence(cleaned_text)
    tagger.predict(sentence)
    
    # Extract and store relevant entities
    for entity in sentence.get_spans("ner"):
        if entity.get_label("ner").value in ['ORG', 'LOC', 'PER']:  # Use relevant entity types
            entity_type = entity.get_label("ner").value
            entity_tuple = (entity.text.strip(), entity_type, story_id)
            all_entities.add(entity_tuple)
            unique_entities_set.add((entity.text.strip(), entity_type))
            
            # Update counts for summary
            type_counts[entity_type] = type_counts.get(entity_type, 0) + 1
            total_count += 1

# Convert sets to sorted DataFrames for saving to Excel
unique_entities_df = pd.DataFrame(sorted(unique_entities_set), columns=["Entity", "Tag"])
all_entities_df = pd.DataFrame(sorted(all_entities), columns=["Entity", "Tag", "Story ID"])
summary_df = pd.DataFrame(list(type_counts.items()), columns=["Tag", "Count"])
summary_df = pd.concat([summary_df, pd.DataFrame([["TOTAL", total_count]], columns=["Tag", "Count"])])

# Save to Excel with multiple sheets
with pd.ExcelWriter("recognized_entities_flair.xlsx", engine="openpyxl") as writer:
    unique_entities_df.to_excel(writer, sheet_name="Unique Entities", index=False)
    all_entities_df.to_excel(writer, sheet_name="All Entities with Story ID", index=False)
    summary_df.to_excel(writer, sheet_name="Summary", index=False)

# Close the database connection
cursor.close()
conn.close()

# Print execution time
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")