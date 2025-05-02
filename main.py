from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
from typing import Dict, Set
import asyncio

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConnectionManager:
    def __init__(self):
        self.active: Dict[WebSocket, str] = {}
        self.presence_update_task = None
        self.presence_update_interval = 1.0  # Update presence every second

    async def connect(self, ws: WebSocket):
        await ws.accept()

    async def register(self, ws: WebSocket, name: str):
        self.active[ws] = name
        print(f"[Join] {name} connected.")
        await self.broadcast_presence()

        # Start presence update task if not already running
        if self.presence_update_task is None:
            self.presence_update_task = asyncio.create_task(self._periodic_presence_update())

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            name = self.active[ws]
            del self.active[ws]
            print(f"[Disconnect] {name} disconnected.")

            # Stop presence update task if no clients left
            if not self.active and self.presence_update_task:
                self.presence_update_task.cancel()
                self.presence_update_task = None

    async def _periodic_presence_update(self):
        try:
            while True:
                await self.broadcast_presence()
                await asyncio.sleep(self.presence_update_interval)
        except asyncio.CancelledError:
            pass

    async def broadcast_presence(self):
        payload = {"type": "presence", "clients": list(self.active.values())}
        data = json.dumps(payload)
        disconnected_clients = set()

        for client in self.active:
            try:
                await client.send_text(data)
            except Exception:
                disconnected_clients.add(client)

        # Clean up disconnected clients
        for client in disconnected_clients:
            self.disconnect(client)

    async def send_to(self, sender: WebSocket, recipient_name: str, payload: dict):
        sender_name = self.active.get(sender)

        if sender_name == recipient_name:
            await sender.send_text(json.dumps({
                "type": "error",
                "message": "You cannot send a file to yourself."
            }))
            print(f"[Warning] {sender_name} tried to send to themselves.")
            return

        data = json.dumps(payload)
        found = False
        for ws, name in self.active.items():
            if name == recipient_name:
                try:
                    await ws.send_text(data)
                    found = True
                    print(f"[Info] Message sent from {sender_name} to {recipient_name}")

                    # If this is a file request, send acknowledgment
                    if payload.get("type") == "file_request":
                        await sender.send_text(json.dumps({
                            "type": "file_request_ack",
                            "to": sender_name,
                            "from": recipient_name,
                            "filename": payload.get("filename"),
                            "filesize": payload.get("filesize")
                        }))
                    break
                except Exception as e:
                    print(f"[Error] Failed to send to {recipient_name}: {str(e)}")
                    found = False
                    break

        if not found:
            await sender.send_text(json.dumps({
                "type": "error",
                "message": f"User '{recipient_name}' not found or not connected."
            }))
            print(f"[Error] Recipient '{recipient_name}' not found. Active users: {list(self.active.values())}")

    async def broadcast_message(self, sender: WebSocket, payload: dict):
        data = json.dumps(payload)
        disconnected_clients = set()

        for client in self.active:
            if client != sender:
                try:
                    await client.send_text(data)
                except Exception:
                    disconnected_clients.add(client)

        # Clean up disconnected clients
        for client in disconnected_clients:
            self.disconnect(client)


manager = ConnectionManager()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        join = json.loads(await ws.receive_text())
        if join.get("type") != "join" or "name" not in join:
            await ws.close(code=1003)
            return

        await manager.register(ws, join["name"])

        while True:
            msg = json.loads(await ws.receive_text())
            msg_type = msg.get("type")

            if msg_type in {"file_request", "file_response", "file_chunk"}:
                await manager.send_to(ws, msg["to"], msg)

                # Handle file chunk acknowledgment
                if msg_type == "file_chunk":
                    await ws.send_text(json.dumps({
                        "type": "file_chunk_ack",
                        "chunk_id": msg.get("chunk_id")
                    }))

            elif msg_type == "chat":
                await manager.broadcast_message(ws, msg)

    except WebSocketDisconnect:
        manager.disconnect(ws)
        await manager.broadcast_presence()
    except Exception as e:
        print(f"[Error] WebSocket error: {str(e)}")
        manager.disconnect(ws)
        await manager.broadcast_presence()
