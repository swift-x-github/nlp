import Pkg; Pkg.add("YAML")
import Pkg; Pkg.add("XLSX")
using LibPQ
using DataFrames
using YAML
using XLSX
using WordTokenizers
using TextModels
using Dates

# Load configuration from YAML file
config = YAML.load_file("../python/config.yaml")

# Database connection parameters
db_config = config["local_db"]

# Connect to the database
conn = LibPQ.Connection(
    "host=$(db_config["host"]) port=$(db_config["port"]) dbname=$(db_config["database"]) " *
    "user=$(db_config["user"]) password=$(db_config["password"])"
)

# Define query parameters from config
query_limit = get(config, "query_julia_limit", 2500000)
offset = get(config, "offset", 0)
query_text = "gold mine"

# SQL query to retrieve stories containing the keyword "gold mine"
story_query = "SELECT id, story FROM public.\"DJ_NEWS_STORIES\" WHERE story LIKE '%$query_text%' LIMIT $query_limit OFFSET $offset;"

# Function to clean text by removing URLs, HTML tags, punctuation, numbers, and extra whitespace
function clean_text(text::String)
    text = replace(text, r"<.*?>" => "") # Remove HTML tags
    text = replace(text, r"[\d]" => "") # Remove numbers
    text = replace(text, r"[^\w\s]" => "") # Remove punctuation, keep words and spaces
    text = strip(text) # Trim whitespace
    return text
end

# Function to extract unique named entities using NERTagger
function extract_named_entities(text::String)
    cleaned_text = clean_text(text)
    tokens = WordTokenizers.tokenize(cleaned_text) # Tokenize cleaned text
    ner_tagger = NERTagger() # Initialize NER tagger
    tags = ner_tagger(tokens) # Perform NER tagging

    # Filter out entities with the tag "MIS" and return unique entities
    unique_entities = Set([(token, tag) for (token, tag) in zip(tokens, tags) if tag != "O" ])
    return unique_entities
end

# Start measuring time
start_time = now()

# Execute query to retrieve stories
result = execute(conn, story_query)
data = DataFrame(result)

# Initialize a set for unique entities and dictionary for unique entity counts
unique_entities_set = Set{Tuple{String, String}}()
type_counts = Dict{String, Int}()

# Process each story and extract unique named entities
for row in eachrow(data)
    story_text = row[:story]

    # Extract entities
    named_entities = extract_named_entities(story_text)

    # Collect unique entities and count only unique instances per tag
    for (token, tag) in named_entities
        if !((token, tag) in unique_entities_set) # Only count if the entity is unique
            push!(unique_entities_set, (token, tag)) # Add to unique entities set
            type_counts[tag] = get(type_counts, tag, 0) + 1 # Increment count for unique entities
        end
    end
end

# Calculate the total count of unique entities
total_count = sum(values(type_counts))

# Convert collected unique entities to DataFrame
unique_entities_df = DataFrame(Entity = [e[1] for e in unique_entities_set], Tag = [e[2] for e in unique_entities_set])

# Write data to Excel file
XLSX.openxlsx("recognized_entities_julia.xlsx", mode="w") do xf
    # Sheet 1: Unique Entities
    sheet1 = XLSX.addsheet!(xf, "Unique Entities")
    XLSX.writetable!(sheet1, unique_entities_df) # Write unique entities
    
    # Sheet 2: Summary
    summary_df = DataFrame(Tag = collect(keys(type_counts)), Count = collect(values(type_counts)))
    push!(summary_df, ("TOTAL", total_count))
    sheet2 = XLSX.addsheet!(xf, "Summary")
    XLSX.writetable!(sheet2, summary_df) # Write summary
end

# Close the database connection
close(conn)

# Print execution time
end_time = now()
println("Execution time: $(end_time - start_time)")
