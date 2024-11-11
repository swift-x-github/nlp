import psycopg2
import yaml
import nltk
import re
import time
from bs4 import BeautifulSoup
from xlsxwriter import Workbook
from nltk import word_tokenize, pos_tag, ne_chunk

# Ensure necessary NLTK data files are downloaded
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')
nltk.download('punkt')

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
query_limit = config.get('query_nltk_limit', 2500000)
offset = config.get('offset', 0)
query_text = 'gold mine'

# SQL query to retrieve stories containing the keyword "gold mine"
story_query = f"SELECT id, story FROM public.\"DJ_NEWS_STORIES\" WHERE story LIKE '%{query_text}%' LIMIT {query_limit} OFFSET {offset};"

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

# Collect unique entities and counts
unique_entities = set()
type_counts = {}

# Process each story in the results
for row in result:
    story_id = row[0]
    text = row[1]
    
    # Clean the story text
    cleaned_text = clean_text(text)
    
    # Tokenize and POS-tag the cleaned text for NLTK NER
    tokens = word_tokenize(cleaned_text)
    tagged = pos_tag(tokens)
    tree = ne_chunk(tagged)
    
    # Extract and store relevant entities
    for subtree in tree:
        if hasattr(subtree, 'label'):
            entity_type = subtree.label()
            entity_text = " ".join([leaf[0] for leaf in subtree.leaves()])
            if entity_type in ['PERSON', 'ORGANIZATION', 'GPE']:
                # Add to unique_entities set
                entity = (entity_text.strip(), entity_type)
                if entity not in unique_entities:
                    unique_entities.add(entity)
                    
                    # Update counts for summary based on unique entities only
                    type_counts[entity_type] = type_counts.get(entity_type, 0) + 1

# Calculate the total count of unique entities
total_count = sum(type_counts.values())

# Sort unique entities alphabetically
unique_entities = sorted(unique_entities, key=lambda x: x[0])

# Prepare Excel file with unique entities and summary only
workbook = Workbook("recognized_entities_nltk.xlsx")

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
