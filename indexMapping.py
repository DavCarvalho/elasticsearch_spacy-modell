indexMapping = {
    "settings": {
        "analysis": {
            "filter": {
                "synonym_filter": {
                    "type": "synonym",
                    "synonyms_path": "sinonimos_output-2.txt"  # Atualize este caminho
                },
                "portuguese_stop": {
                    "type": "stop",
                    "stopwords": "_portuguese_"
                },
            },
            "analyzer": {
                "synonym_analyzer": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "synonym_filter","portuguese_stop"]
                }
            }
        }
    },
    "mappings": {
        "properties": {  # Adicione esta linha
            "fileName": {"type": "text", "analyzer": "synonym_analyzer"},
            "page": {"type": "integer"},
            "content": {"type": "text", "analyzer": "synonym_analyzer"},
        }
    }
}