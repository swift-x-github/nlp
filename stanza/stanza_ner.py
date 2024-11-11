import psycopg2
import yaml
import stanza
import pandas as pd
import re
from bs4 import BeautifulSoup
import time

# Load configuration from YAML file
with open('../python/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Database connection parameters
db_config = config['local_db']

# Connect to the database
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    dbname=db_config['database'],
    user=db_config['user'],
    password=db_config['password']
)

# Define query parameters
query_limit = config.get('query_stanza_limit', 2500000)
offset = config.get('offset', 0)
query_text = 'gold mine'

# SQL query to retrieve stories containing the keyword "gold mine"
story_query = f"SELECT id, story FROM public.\"DJ_NEWS_STORIES\" WHERE story LIKE '%{query_text}%' LIMIT {query_limit} OFFSET {offset};"

# Load Stanza's English model for NER
stanza.download('en')
nlp = stanza.Pipeline('en', processors='tokenize,ner')

# Function to clean text: removes URLs, HTML tags, punctuation, numbers, and extra whitespace
def clean_text(text):
    text = re.sub(r'https?://\S+|www\.\S+', '', text)  # Remove URLs
    text = BeautifulSoup(text, "html.parser").get_text()  # Remove HTML tags
    text = re.sub(r'[^a-zA-Z\s]', '', text)  # Keep only letters and spaces
    text = re.sub(r'\s+', ' ', text).strip()  # Remove extra whitespace
    return text

# Start processing
start_time = time.time()

# Execute query to retrieve stories
cursor = conn.cursor()
cursor.execute(story_query)
result = cursor.fetchall()

# Collect unique entities and counts
unique_entities_set = set()
type_counts = {}

# Process each story in the results
for row in result:
    story_id = row[0]
    text = row[1]
    
    # Clean the story text
    cleaned_text = clean_text(text)
    
    # Process cleaned text with Stanza NER
    doc = nlp(cleaned_text)
    
    # Extract and store relevant entities
    for ent in doc.ents:
        if ent.type in ['ORG', 'GPE', 'PERSON']:  # Use relevant entity types
            entity_tuple = (ent.text.strip(), ent.type)
            if entity_tuple not in unique_entities_set:
                unique_entities_set.add(entity_tuple)  # Add to unique entities set
                type_counts[ent.type] = type_counts.get(ent.type, 0) + 1  # Count only unique entities

# Calculate the total count of unique entities
total_count = sum(type_counts.values())

# Convert the unique entities to a DataFrame
unique_entities_df = pd.DataFrame(sorted(unique_entities_set), columns=["Entity", "Tag"])

# Create a summary DataFrame with total unique entities per tag
summary_df = pd.DataFrame(list(type_counts.items()), columns=["Tag", "Count"])
summary_df = pd.concat([summary_df, pd.DataFrame([["TOTAL", total_count]], columns=["Tag", "Count"])])

# Save to Excel with only the unique entities and summary sheets
with pd.ExcelWriter("recognized_entities_stanza.xlsx", engine="openpyxl") as writer:
    unique_entities_df.to_excel(writer, sheet_name="Unique Entities", index=False)
    summary_df.to_excel(writer, sheet_name="Summary", index=False)

# Close the database connection
cursor.close()
conn.close()

# Print execution time
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
