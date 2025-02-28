#!/bin/bash

# Abilita il controllo degli errori
set -e

# Funzione per controllare ed eventualmente creare un topic
create_topic() {
    local TOPIC=$1
    echo "Verifica del topic '$TOPIC'..."

    kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -wq "$TOPIC"
    if [ $? -eq 0 ]; then
        echo "✅ Il topic '$TOPIC' esiste già."
    else
        echo "⚠ Creazione del topic '$TOPIC'..."
        kafka-topics.sh --create --topic "$TOPIC" --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
        echo "✅ Topic '$TOPIC' creato con successo."
    fi
}

# Controlla e crea i topic necessari
create_topic "fake-news"
create_topic "news-topics"

echo "🎯 Tutti i topic sono stati verificati o creati correttamente."