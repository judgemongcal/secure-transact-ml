import pandas as pd
import asyncio
from fastapi import FastAPI, WebSocket

app = FastAPI()


df = pd.read_csv("data/transactions.csv")

async def root():
    return {"message": "WebSocket server is running. Connect to /ws/transactions"}

@app.websocket("/ws/transactions")

async def stream_txns(websocket: WebSocket):
    await websocket.accept()

    # Stream data row by row
    for _, row in df.iterrows():
        txn_data = row.to_dict();
        await websocket.send_json(txn_data)


        await asyncio.sleep(1)

    await websocket.close()