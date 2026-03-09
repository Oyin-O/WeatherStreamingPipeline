#!/bin/bash

# ── colours for pretty output ──────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

PROJECT_DIR="/Users/Oyindamola/Documents/Personal Project/WeatherStreamingPipeline"
VENV="$PROJECT_DIR/venv/bin/activate"

echo -e "${GREEN}🌦  Starting Weather Streaming Pipeline...${NC}"
echo "────────────────────────────────────────"

# ── Step 1: Start Kafka ────────────────────────────────────────
echo -e "${YELLOW}▶ Starting Kafka & Zookeeper...${NC}"
cd "$PROJECT_DIR"
docker compose up -d
echo -e "${GREEN}✅ Kafka started${NC}"

# ── Step 2: Wait for Kafka to be ready ────────────────────────
echo -e "${YELLOW}⏳ Waiting for Kafka to be ready...${NC}"
sleep 10
echo -e "${GREEN}✅ Kafka ready${NC}"

# ── Step 3: Start Producer ─────────────────────────────────────
echo -e "${YELLOW}▶ Starting Weather Producer...${NC}"
osascript -e "
tell application \"Terminal\"
    do script \"cd '$PROJECT_DIR' && source '$VENV' && python -m producer.weather_producer\"
end tell"
echo -e "${GREEN}✅ Producer started${NC}"

# ── Step 4: Wait for producer to publish first batch ──────────
echo -e "${YELLOW}⏳ Waiting for producer to publish first batch...${NC}"
sleep 5

# ── Step 5: Start Spark Consumer ──────────────────────────────
echo -e "${YELLOW}▶ Starting Spark Consumer...${NC}"
osascript -e "
tell application \"Terminal\"
    do script \"cd '$PROJECT_DIR' && source '$VENV' && python -m consumer.spark_consumer\"
end tell"
echo -e "${GREEN}✅ Spark Consumer started${NC}"

# ── Step 6: Wait for Spark to process first batch ─────────────
echo -e "${YELLOW}⏳ Waiting for Spark to process first batch...${NC}"
sleep 15

# ── Step 7: Start Streamlit Dashboard ─────────────────────────
echo -e "${YELLOW}▶ Starting Streamlit Dashboard...${NC}"
osascript -e "
tell application \"Terminal\"
    do script \"cd '$PROJECT_DIR' && source '$VENV' && streamlit run app.py\"
end tell"
echo -e "${GREEN}✅ Dashboard started${NC}"

# ── Step 8: Start Data Quality Monitor ────────────────────────
echo -e "${YELLOW}▶ Starting Data Quality Monitor...${NC}"
osascript -e "
tell application \"Terminal\"
    do script \"cd '$PROJECT_DIR' && source '$VENV' && python -m consumer.monitor\"
end tell"
echo -e "${GREEN}✅ Data Quality Monitor started${NC}"

# ── Step 9: Start Scheduler (daily summary) ───────────────────
echo -e "${YELLOW}▶ Starting Scheduler...${NC}"
osascript -e "
tell application \"Terminal\"
    do script \"cd '$PROJECT_DIR' && source '$VENV' && python -m consumer.scheduler\"
end tell"
echo -e "${GREEN}✅ Scheduler started${NC}"

echo ""
echo "────────────────────────────────────────"
echo -e "${GREEN}🎉 Pipeline is running!${NC}"
echo ""
echo "  📊 Dashboard:   http://localhost:8501"
echo "  ⚡ Spark UI:    http://localhost:4040"
echo "  🐳 Kafka:       localhost:9092"
echo ""
echo "  Terminals running:"
echo "  1. Weather Producer"
echo "  2. Spark Consumer"
echo "  3. Streamlit Dashboard"
echo "  4. Data Quality Monitor"
echo "  5. Scheduler"
echo ""
echo -e "${YELLOW}To stop everything run: ./stop.sh${NC}"