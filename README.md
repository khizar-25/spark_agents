git clone https://github.com/khizar-25/spark_agents.git
cd spark_agents

Step 2 — Install dependencies
bashpip3 install -r requirements.txt --break-system-packages

Step 3 — Set your Anthropic API key
bashexport ANTHROPIC_API_KEY="sk-ant-xxxxxxxxxxxxxxxx"
Or edit the .env file directly:
bashnano .env
# Add this line:
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxxxxx
# Ctrl+X → Y → Enter

Step 4 — Run the agent
bashchmod +x run.sh
./run.sh
Or run directly with Python:
bashpython3 main.py

If on KillerCoda — full one-liner setup:
bashgit clone https://github.com/khizar-25/spark_agents.git && \
cd spark_agents && \
pip3 install -r requirements.txt --break-system-packages && \
export ANTHROPIC_API_KEY="sk-ant-xxxxxxxxxxxxxxxx" && \
python3 main.py
