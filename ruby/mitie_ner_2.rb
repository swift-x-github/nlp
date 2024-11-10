require 'pg'
require 'yaml'
require 'mitie'
require 'benchmark'

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

# Start processing and collecting entities
time = Benchmark.realtime do
  result = conn.exec(story_query)
  
  # Collect entities with story_id in an array
  all_entities = []
  total_count = 0
  type_counts = Hash.new(0)

  result.each do |row|
    story_id = row['id']
    text = row['story']
    
    # Process the story with NER
    doc = ner_model.doc(text)
    
    # Extract and clean entities
    entities = doc.entities
                  .select { |entity| ['ORGANIZATION', 'LOCATION'].include?(entity[:tag]) }
                  .map do |entity| 
                    {
                      text: entity[:text].strip.squeeze(" "),  # Remove extra spaces
                      tag: entity[:tag],
                      story_id: story_id
                    }
                  end
                  .uniq
    
    # Add entities to the collection and update counts
    all_entities.concat(entities)
    entities.each do |entity|
      type_counts[entity[:tag]] += 1
      total_count += 1
    end
  end

  # Sort entities alphabetically by their text
  sorted_entities = all_entities.sort_by { |entity| entity[:text] }

  # Output sorted entities with story IDs
  sorted_entities.each do |entity|
    puts "Entity: #{entity[:text]}, Tag: #{entity[:tag]}, Story ID: #{entity[:story_id]}"
  end

  # Output summary counts
  puts "\n--- Entity Summary ---"
  puts "TOTAL ROWS" + query_limit.to_s
  type_counts.each do |tag, count|
    puts "#{tag}: #{count}"
  end
  puts "TOTAL NUMBER OF ENTITIES: #{total_count}"
end

puts "Execution time: #{time.round(2)} seconds"
