import json

# Percorsi dei file
input_file = "fake-news-index.json"
output_file = "fake-news-index.ndjson"

# Legge il file JSON esportato
with open(input_file, "r", encoding="utf-8") as f:
    try:
        data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Errore nel parsing del JSON: {e}")
        exit(1)

# Verifica la struttura attesa
documents = data.get("response", {}).get("hits", {}).get("hits", [])
if not isinstance(documents, list):
    print("Errore: Struttura JSON non conforme. Verifica il file di input.")
    exit(1)

# Scriviamo il file in formato NDJSON
with open(output_file, "w", encoding="utf-8") as f:
    for hit in documents:
        index_name = hit.get("_index", "unknown_index")
        doc_id = hit.get("_id", "unknown_id")
        fields = hit.get("fields", {})

        # Controlliamo e normalizziamo i campi
        def get_first_value(field, default):
            value = fields.get(field, default)
            return value[0] if isinstance(value, list) and value else value

        title = get_first_value("title", "No Title")
        content = get_first_value("content", "No Content")
        fake_news_rating = get_first_value("fake_news_rating", "Unknown")

        # Creiamo il documento formattato correttamente
        source = {
            "title": title,
            "content": content,
            "fake_news_rating": fake_news_rating
        }

        # Scriviamo nel file NDJSON
        f.write(json.dumps({"index": {"_index": index_name, "_id": doc_id}}) + "\n")
        f.write(json.dumps(source) + "\n")

print(f"Conversione completata. File salvato come {output_file}")
