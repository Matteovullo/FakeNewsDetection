import json

# Percorsi dei file
input_file = "news-topics-index.json"
output_file = "news-topics-index.ndjson"

# Legge il file JSON esportato
with open(input_file, "r") as f:
    data = json.load(f)

# Scriviamo il file in formato NDJSON
with open(output_file, "w") as f:
    for hit in data.get("response", {}).get("hits", {}).get("hits", []):
        index_name = hit["_index"]
        doc_id = hit["_id"]
        fields = hit.get("fields", {})

        # Controlliamo i campi, assegnando valori predefiniti se mancano
        title = fields.get("title", ["No Title"])[0]
        content = fields.get("content", ["No Content"])[0]
        bigrams = fields.get("bigrams", [""])[0]
        topic_distribution = fields.get("topicDistribution", [0])[0]

        # Creiamo il documento formattato correttamente
        source = {
            "title": title,
            "content": content,
            "bigrams": bigrams,
            "topicDistribution": topic_distribution
        }

        # Scriviamo nel file NDJSON
        f.write(json.dumps({"index": {"_index": index_name, "_id": doc_id}}) + "\n")
        f.write(json.dumps(source) + "\n")
