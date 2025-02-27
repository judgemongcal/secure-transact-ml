import pandas as pd
import asyncio
from fastapi import FastAPI, WebSocket
import kagglehub
from kagglehub import KaggleDatasetAdapter
from kafka.producer import send_to_kafka


app = FastAPI()

file_path = "credit_card_transactions.csv"

df = kagglehub.load_dataset(KaggleDatasetAdapter.PANDAS, 
                            "priyamchoksi/credit-card-transactions-dataset",
                            file_path)

print(df.head())
async def root():
    return {"message": "WebSocket server is running. Connect to /ws/transactions"}

@app.websocket("/ws/transactions")

async def stream_txns(websocket: WebSocket):
    await websocket.accept()
    batch_size = 100
    batch = []

    # Stream data row by row
    for _, row in df.iterrows():
        txn_data = row.to_dict();
        batch.append(txn_data)

        if(len(batch)>= batch_size):
            # await websocket.send_json(batch) 
            print(f"Sending batch of {len(batch)} transactions to Kafka..")
            send_to_kafka("transactions", str(batch))
            batch = []

        await asyncio.sleep(1)

    if batch:
        # await websocket.send_json(batch)
        send_to_kafka("transactions", str(batch))
        
    await websocket.close()

