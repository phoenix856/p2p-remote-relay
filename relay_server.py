#!/usr/bin/env python3
"""
P2P Remote Desktop - WebSocket Relay Server
Deploy on Render.com (free tier) for universal connectivity.
Both sender and receiver connect OUTBOUND to this server,
so no firewall rules, port forwarding, or router config is needed.
"""
import asyncio
import json
import hashlib
import time
import os
import websockets

_sessions: dict = {}


async def _pipe(src, dst):
    try:
        async for msg in src:
            await dst.send(msg)
    except Exception:
        pass
    try:
        await dst.close()
    except Exception:
        pass


async def handler(websocket, path=None):
    session_key = None
    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=30)
        if isinstance(raw, bytes):
            raw = raw.decode()
        data = json.loads(raw)

        role     = data.get("role", "").lower()
        password = data.get("password", "")

        if role not in ("sender", "receiver") or not password:
            await websocket.send(json.dumps({"status": "error", "msg": "invalid_header"}))
            return

        session_key = hashlib.sha256(password.encode()).hexdigest()[:16]

        if session_key in _sessions and _sessions[session_key]["role"] != role:
            session       = _sessions.pop(session_key)
            partner_ws    = session["ws"]
            partner_event = session["event"]

            session["partner_ws"] = websocket
            partner_event.set()

            await websocket.send(json.dumps({"status": "connected"}))

            await asyncio.gather(
                _pipe(websocket, partner_ws),
                _pipe(partner_ws, websocket),
            )
            session_key = None

        else:
            event = asyncio.Event()
            _sessions[session_key] = {
                "ws": websocket,
                "role": role,
                "event": event,
                "partner_ws": None,
                "ts": time.time(),
            }

            await websocket.send(json.dumps({"status": "waiting"}))
            print(f"[~] {role} waiting  key={session_key}")

            try:
                await asyncio.wait_for(event.wait(), timeout=180)
            except asyncio.TimeoutError:
                await websocket.send(json.dumps({"status": "error", "msg": "timeout"}))
                return

            await websocket.send(json.dumps({"status": "connected"}))
            print(f"[+] Session matched key={session_key}")

            await websocket.wait_closed()
            session_key = None

    except Exception:
        pass
    finally:
        if session_key:
            _sessions.pop(session_key, None)


async def _stale_cleaner():
    while True:
        await asyncio.sleep(30)
        now   = time.time()
        stale = [k for k, v in list(_sessions.items()) if now - v["ts"] > 180]
        for k in stale:
            try:
                await _sessions[k]["ws"].close()
            except Exception:
                pass
            _sessions.pop(k, None)


async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"[+] P2P Relay Server running on port {port}")
    asyncio.create_task(_stale_cleaner())
    async with websockets.serve(handler, "0.0.0.0", port,
                                ping_interval=20, ping_timeout=60):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
