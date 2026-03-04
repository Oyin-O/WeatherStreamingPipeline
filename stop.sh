#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PROJECT_DIR="/Users/Oyindamola/Documents/Personal Project/WeatherStreamingPipeline"

echo -e "${YELLOW}🛑 Stopping Weather Streaming Pipeline...${NC}"
echo "────────────────────────────────────────"

# Stop Kafka
echo -e "${YELLOW}▶ Stopping Kafka...${NC}"
cd "$PROJECT_DIR"
docker compose down
echo -e "${GREEN}✅ Kafka stopped${NC}"

# Kill Python processes
echo -e "${YELLOW}▶ Stopping Producer & Spark Consumer...${NC}"
pkill -f "weather_producer"
pkill -f "sparkconsumer"
pkill -f "streamlit"
echo -e "${GREEN}✅ All Python processes stopped${NC}"

echo ""
echo -e "${GREEN}✅ Pipeline stopped successfully!${NC}"