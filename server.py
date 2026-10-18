import asyncio
import random
import secrets
import time
from typing import Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

app = FastAPI(title="MindPlay Online")

rooms: Dict[str, Dict[str, Any]] = {}
connections: Dict[str, WebSocket] = {}
PLAYER_TOKENS: Dict[str, str] = {}

@app.get("/")
async def home():
    return FileResponse("index.html")

def public_room(room):
    return {
        "code": room["code"],
        "players": [{"id": p["id"], "name": p["name"], "connected": p["connected"]} for p in room["players"].values()],
        "status": room["status"],
        "question": room.get("question"),
        "ends_at": room.get("ends_at"),
        "game_mode": room.get("game_mode"),
    }

async def send_room(code, payload):
    room = rooms.get(code)
    if not room:
        return
    for pid in list(room["players"]):
        ws = connections.get(pid)
        if ws:
            try:
                await ws.send_json(payload)
            except Exception:
                pass

async def broadcast_state(code):
    if code in rooms:
        await send_room(code, {"type": "state", "room": public_room(rooms[code])})

def make_question():
    a, b = random.randint(2, 20), random.randint(2, 20)
    op = random.choice(["+", "-", "×"])
    answer = a + b if op == "+" else a - b if op == "-" else a * b
    return {"text": f"{a} {op} {b} = ?", "answer": answer}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    pid = None
    room_code = None
    try:
        while True:
            msg = await ws.receive_json()
            kind = msg.get("type")

            if kind == "create":
                name = str(msg.get("name", "Pemain")).strip()[:24] or "Pemain"
                room_code = secrets.token_hex(2).upper()
                pid = secrets.token_hex(8)
                token = secrets.token_urlsafe(20)
                rooms[room_code] = {
                    "code": room_code,
                    "players": {pid: {"id": pid, "name": name, "connected": True, "token": token, "answer": None, "submitted_at": None}},
                    "status": "lobby",
                    "question": None,
                    "ends_at": None,
                    "game_mode": "numeric",
                    "created_at": time.time(),
                }
                connections[pid] = ws
                PLAYER_TOKENS[token] = pid
                await ws.send_json({"type": "welcome", "id": pid, "token": token, "code": room_code})
                await broadcast_state(room_code)

            elif kind == "join":
                code = str(msg.get("code", "")).strip().upper()
                name = str(msg.get("name", "Pemain")).strip()[:24] or "Pemain"
                token = str(msg.get("token", ""))
                room = rooms.get(code)
                if token and token in PLAYER_TOKENS:
                    existing_pid = PLAYER_TOKENS[token]
                    for c, r in rooms.items():
                        if existing_pid in r["players"]:
                            room = r
                            code = c
                            break
                    if room and existing_pid in room["players"]:
                        pid = existing_pid
                        room_code = code
                        room["players"][pid]["connected"] = True
                        room["players"][pid]["name"] = name or room["players"][pid]["name"]
                        connections[pid] = ws
                        await ws.send_json({"type": "welcome", "id": pid, "token": token, "code": code})
                        await broadcast_state(code)
                        continue
                if not room:
                    await ws.send_json({"type": "error", "message": "Ruangan tidak ditemukan. Periksa kode ruangan."})
                    continue
                if room["status"] != "lobby":
                    await ws.send_json({"type": "error", "message": "Permainan sudah dimulai. Untuk prototipe ini, bergabung hanya saat lobi."})
                    continue
                if len(room["players"]) >= 10:
                    await ws.send_json({"type": "error", "message": "Ruangan sudah penuh (maksimal 10 pemain)."})
                    continue
                pid = secrets.token_hex(8)
                token = secrets.token_urlsafe(20)
                room_code = code
                room["players"][pid] = {"id": pid, "name": name, "connected": True, "token": token, "answer": None, "submitted_at": None}
                connections[pid] = ws
                PLAYER_TOKENS[token] = pid
                await ws.send_json({"type": "welcome", "id": pid, "token": token, "code": code})
                await broadcast_state(code)

            elif kind == "start" and pid and room_code in rooms:
                room = rooms[room_code]
                if room["status"] != "lobby":
                    continue
                room["game_mode"] = "numeric"
                room["question"] = make_question()
                room["status"] = "playing"
                room["ends_at"] = time.time() + 45
                for p in room["players"].values():
                    p["answer"] = None
                    p["submitted_at"] = None
                await broadcast_state(room_code)

            elif kind == "answer" and pid and room_code in rooms:
                room = rooms[room_code]
                if room["status"] != "playing" or time.time() > room["ends_at"]:
                    await ws.send_json({"type": "error", "message": "Waktu habis atau soal belum dimulai."})
                    continue
                player = room["players"].get(pid)
                if player and player["answer"] is None:
                    try:
                        answer = float(msg.get("answer"))
                    except (TypeError, ValueError):
                        await ws.send_json({"type": "error", "message": "Masukkan angka yang valid."})
                        continue
                    player["answer"] = answer
                    player["submitted_at"] = time.time()
                    actual = room["question"]["answer"]
                    results = []
                    for p in room["players"].values():
                        if p["answer"] is not None:
                            results.append({
                                "name": p["name"],
                                "difference": abs(p["answer"] - actual),
                                "seconds": round(p["submitted_at"] - (room["ends_at"] - 45), 2),
                            })
                    results.sort(key=lambda r: (r["difference"], r["seconds"]))
                    await send_room(room_code, {"type": "answer_received", "results": results})
                    if all(p["answer"] is not None or not p["connected"] for p in room["players"].values()):
                        room["status"] = "finished"
                        await send_room(room_code, {"type": "finished", "results": results, "actual": actual})
                        await broadcast_state(room_code)

            elif kind == "new_round" and pid and room_code in rooms:
                room = rooms[room_code]
                room["question"] = None
                room["status"] = "lobby"
                room["ends_at"] = None
                await broadcast_state(room_code)

    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if pid:
            connections.pop(pid, None)
            if room_code in rooms and pid in rooms[room_code]["players"]:
                rooms[room_code]["players"][pid]["connected"] = False
                await broadcast_state(room_code)
