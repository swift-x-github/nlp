require 'pg'
require 'yaml'
require 'mitie'
require 'benchmark'
require 'nokogiri'     # For HTML tag removal
require 'write_xlsx'   # For Excel file creation
require 'set'          # For managing unique entities

# Load configuration
config_path = File.expand_path('../python/config.yaml', __dir__)
config = YAML.load_file(config_path)

# Choose the database to connect to (e.g., local_db or remote_db)
db_config = config['local_db']  # Adjust to 'remote_db' if needed

# Database connection
conn = PG.connect(
  host: db_config['host'],
  port: db_config['port'],
  dbname: db_config['database'],
  user: db_config['user'],
  password: db_config['password']
)

# Define the query for extracting stories
query_limit = config['query_mitie_limit'] || 2500000
offset = config['offset'] || 0
query_text = 'gold mine'

story_query = "SELECT id, story FROM public.\"DJ_NEWS_STORIES\" WHERE story LIKE '%#{query_text}%' LIMIT #{query_limit} OFFSET #{offset};"

# Load NER model for entity recognition
ner_model_path = './MITIE-models-v0.2/MITIE-models/english/ner_model.dat'
ner_model = Mitie::NER.new(ner_model_path)

# Method to clean the text of URLs, HTML tags, punctuation, digits, and non-printable characters
def clean_text(text)
  # Remove URLs
  text = text.gsub(%r{https?://\S+|www\.\S+}, '')

  # Remove HTML tags
  text = Nokogiri::HTML(text).text

  # Remove punctuation, quotation marks, apostrophes, digits, and non-printable characters
  text = text.gsub(/[^a-zA-Z\s]/, '')  # Keep only letters and spaces
  text = text.gsub(/\s+/, ' ').strip   # Remove extra whitespace

  text
end

# Start processing and collecting entities
time = Benchmark.realtime do
  result = conn.exec(story_query)
  
  # Collect unique entities
  unique_entities = Set.new
  type_counts = Hash.new(0)

  result.each do |row|
    story_id = row['id']
    text = row['story']

    # Clean the story text
    cleaned_text = clean_text(text)
    
    # Process the cleaned story with NER
    doc = ner_model.doc(cleaned_text)
    
    # Extract entities
    entities = doc.entities
                  .select { |entity| ['ORGANIZATION', 'LOCATION', 'PERSON'].include?(entity[:tag]) }
                  .map do |entity| 
                    {
                      text: entity[:text].strip.squeeze(" "),  # Remove extra spaces
                      tag: entity[:tag]
                    }
                  end
                  .uniq
    
    # Add unique entities to the set and update type counts
    entities.each do |entity|
      entity_key = "#{entity[:text]}_#{entity[:tag]}"  # Unique identifier for entity
      unless unique_entities.include?(entity_key)
        unique_entities.add(entity_key)
        type_counts[entity[:tag]] += 1
      end
    end
  end

  # Calculate the total count of unique entities
  total_count = unique_entities.size

  # Create Excel workbook and sheets
  workbook = WriteXLSX.new("recognized_entities.xlsx")

  # First sheet: Unique entities
  unique_entities_sheet = workbook.add_worksheet("Unique Entities")
  unique_entities_sheet.write_row(0, 0, ["Entity", "Tag"])
  unique_entities.each_with_index do |entity_key, index|
    text, tag = entity_key.split('_', 2)
    unique_entities_sheet.write_row(index + 1, 0, [text, tag])
  end

  # Second sheet: Summary (only unique entities are counted)
  summary_sheet = workbook.add_worksheet("Summary")
  summary_sheet.write_row(0, 0, ["Tag", "Count"])
  type_counts.each_with_index do |(tag, count), index|
    summary_sheet.write_row(index + 1, 0, [tag, count])
  end
  summary_sheet.write_row(type_counts.size + 1, 0, ["TOTAL", total_count])

  # Close workbook
  workbook.close
end

puts "Execution time: #{time.round(2)} seconds"

