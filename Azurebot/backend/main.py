"""
Aegis.ai Python Backend — Streaming Voice Pipeline

Duplex WebSocket bridge between the C# Teams media bot and the voice pipeline:

    Teams (audio frames, 20ms each, 50/sec)
        ↓ (ACS media streaming WebSocket)
    C# media bot
        ↓ (duplex WebSocket: /voice/sessions/{sessionId})
    Python:
        Deepgram Live (streaming STT)      → stt.partial, stt.final
        OpenAI streaming LLM (OpenRouter)  → llm.partial, llm.final
        ElevenLabs WebSocket TTS           → tts.audio (PCM 16k mono), tts.end

    C# receives tts.audio chunks and writes them back to ACS immediately.
    The bot begins speaking as soon as the first TTS frames arrive — no wait
    for the full answer. On a new user utterance the pipeline emits tts.cancel
    so C# can flush its playout buffer (barge-in).

Wire endpoints:
    WS  /voice/sessions/{sessionId}   duplex audio + events (C# ↔ Python)
    POST /join-meeting                Python → C# — answer/join a call
    POST /leave-meeting               Python → C# — hang up
    GET  /health                      liveness + key status
    GET  /status                      active sessions + recent events

Event contract on the duplex WebSocket (Python ↔ C#):

    C# → Python:
        { "type": "audio.in",   "seq": int, "pcm": base64 }   (PCM 16k mono)
        { "type": "user.start" }                              (optional VAD hint)
        { "type": "user.end" }                                (optional)

    Python → C#:
        { "type": "stt.partial", "text": str }
        { "type": "stt.final",   "text": str }
        { "type": "llm.partial", "text": str }
        { "type": "llm.final",   "text": str }
        { "type": "tts.audio",   "seq": int, "pcm": base64 }
        { "type": "tts.end" }
        { "type": "tts.cancel" }                              (barge-in)
        { "type": "error",       "message": str }
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator

import httpx
import websockets
from botbuilder.core import (
    ActivityHandler,
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity
from botframework.connector.auth import MicrosoftAppCredentials
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from openai import AsyncOpenAI

load_dotenv()

# ─── Config ──────────────────────────────────────────────────

CSHARP_URL = os.getenv("CSHARP_SERVICE_URL", "http://localhost:5000")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "anthropic/claude-3.5-sonnet")
DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY", "")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
ELEVENLABS_MODEL = os.getenv("ELEVENLABS_MODEL", "eleven_turbo_v2_5")
BOT_APP_ID = os.getenv("BOT_APP_ID", "")
BOT_APP_PASSWORD = os.getenv("BOT_APP_PASSWORD", "")
BOT_TENANT_ID = os.getenv("BOT_TENANT_ID", "")
print(
    f"[bot] AppId loaded: {bool(BOT_APP_ID)} ({BOT_APP_ID[:8]}...), "
    f"Secret loaded: {bool(BOT_APP_PASSWORD)} (len={len(BOT_APP_PASSWORD)})"
)

SAMPLE_RATE = 16000

llm_client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

app = FastAPI(title="Aegis.ai Streaming Backend", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SYSTEM_PROMPT = (
    "You are Aegis AI, a professional meeting assistant. "
    "You help with meeting discussions, answer questions, and extract action items. "
    "Keep responses concise — 1 to 3 sentences max."
)


# ─── Session state ───────────────────────────────────────────

@dataclass
class Session:
    """One live Teams call. Holds the audio queue for Deepgram, the
    in-flight LLM/TTS task so we can cancel it on barge-in, and the
    rolling conversation history."""

    id: str
    ws: WebSocket
    history: list[dict] = field(default_factory=lambda: [{"role": "system", "content": SYSTEM_PROMPT}])
    inbound_audio: asyncio.Queue[bytes] = field(default_factory=asyncio.Queue)
    bot_speaking: bool = False
    speak_task: asyncio.Task | None = None

    def trim_history(self, max_turns: int = 20) -> None:
        if len(self.history) > max_turns + 1:
            self.history = [self.history[0]] + self.history[-max_turns:]


sessions: dict[str, Session] = {}
event_log: deque[dict] = deque(maxlen=100)


def log_event(kind: str, **data: Any) -> None:
    event_log.append({"t": time.time(), "kind": kind, **data})


# ─── Streaming STT (Deepgram Live) ───────────────────────────

async def deepgram_stream(session: Session) -> None:
    """Pump inbound PCM frames from the session queue into Deepgram's live
    WebSocket and forward transcripts back to the client (C# media bot)."""

    if not DEEPGRAM_API_KEY:
        print(f"[{session.id}] Deepgram not configured — STT disabled")
        return

    url = (
        "wss://api.deepgram.com/v1/listen"
        f"?model=nova-2&language=en-US&encoding=linear16&sample_rate={SAMPLE_RATE}"
        "&channels=1&punctuate=true&interim_results=true&smart_format=true"
        "&endpointing=300"
    )
    headers = {"Authorization": f"Token {DEEPGRAM_API_KEY}"}

    try:
        async with websockets.connect(url, additional_headers=headers) as dg:

            async def send_audio() -> None:
                while True:
                    chunk = await session.inbound_audio.get()
                    if chunk == b"__END__":
                        await dg.send(json.dumps({"type": "CloseStream"}))
                        return
                    await dg.send(chunk)

            async def recv_transcripts() -> None:
                async for raw in dg:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("type") != "Results":
                        continue

                    alt = msg.get("channel", {}).get("alternatives", [{}])[0]
                    text = alt.get("transcript", "").strip()
                    if not text:
                        continue

                    is_final = bool(msg.get("is_final"))
                    speech_final = bool(msg.get("speech_final"))

                    if is_final and speech_final:
                        await send_event(session, {"type": "stt.final", "text": text})
                        log_event("stt.final", session=session.id, text=text)
                        await handle_user_final(session, text)
                    elif is_final:
                        # keep interim visible to client while we wait for endpoint
                        await send_event(session, {"type": "stt.partial", "text": text})
                    else:
                        await send_event(session, {"type": "stt.partial", "text": text})

            await asyncio.gather(send_audio(), recv_transcripts())

    except Exception as exc:
        print(f"[{session.id}] Deepgram stream error: {exc}")
        await send_event(session, {"type": "error", "message": f"stt: {exc}"})


# ─── Streaming TTS (ElevenLabs WebSocket) ────────────────────

async def tts_stream(
    session: Session,
    text_chunks: AsyncGenerator[str, None],
) -> None:
    """Open an ElevenLabs websocket, feed LLM text as it arrives, and forward
    each returned PCM chunk to the client immediately."""

    if not ELEVENLABS_API_KEY:
        print(f"[{session.id}] ElevenLabs not configured — TTS disabled")
        # still drain the generator so the LLM task finishes
        async for _ in text_chunks:
            pass
        return

    url = (
        f"wss://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/stream-input"
        f"?model_id={ELEVENLABS_MODEL}&output_format=pcm_16000"
    )
    headers = {"xi-api-key": ELEVENLABS_API_KEY}

    seq = 0
    try:
        async with websockets.connect(url, additional_headers=headers) as el:
            # initialise voice settings
            await el.send(json.dumps({
                "text": " ",
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
                "generation_config": {"chunk_length_schedule": [50, 90, 120, 150]},
            }))

            async def pump_text() -> None:
                async for chunk in text_chunks:
                    if chunk:
                        await el.send(json.dumps({"text": chunk, "try_trigger_generation": True}))
                # flush
                await el.send(json.dumps({"text": ""}))

            async def pump_audio() -> None:
                nonlocal seq
                async for raw in el:
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    audio_b64 = msg.get("audio")
                    if audio_b64:
                        seq += 1
                        await send_event(session, {
                            "type": "tts.audio",
                            "seq": seq,
                            "pcm": audio_b64,
                        })

                    if msg.get("isFinal"):
                        break

            await asyncio.gather(pump_text(), pump_audio())

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[{session.id}] TTS stream error: {exc}")
        await send_event(session, {"type": "error", "message": f"tts: {exc}"})


# ─── Streaming LLM ───────────────────────────────────────────

async def llm_stream(session: Session, user_text: str) -> AsyncGenerator[str, None]:
    """Emit LLM tokens as they arrive so TTS can start synthesis early.
    Accumulates the full reply into session.history at the end."""

    session.history.append({"role": "user", "content": user_text})
    session.trim_history()

    full = ""
    if not OPENROUTER_API_KEY:
        fallback = f"[LLM not configured] You said: {user_text}"
        await send_event(session, {"type": "llm.partial", "text": fallback})
        await send_event(session, {"type": "llm.final", "text": fallback})
        session.history.append({"role": "assistant", "content": fallback})
        yield fallback
        return

    try:
        stream = await llm_client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=session.history,
            temperature=0.7,
            max_tokens=200,
            stream=True,
        )

        async for event in stream:
            delta = event.choices[0].delta.content if event.choices else None
            if not delta:
                continue
            full += delta
            await send_event(session, {"type": "llm.partial", "text": delta})
            yield delta

        await send_event(session, {"type": "llm.final", "text": full})
        session.history.append({"role": "assistant", "content": full})
        log_event("llm.final", session=session.id, text=full)

    except Exception as exc:
        print(f"[{session.id}] LLM stream error: {exc}")
        await send_event(session, {"type": "error", "message": f"llm: {exc}"})


# ─── Turn handling ───────────────────────────────────────────

async def handle_user_final(session: Session, text: str) -> None:
    """User finished a turn. Cancel any in-flight bot speech (barge-in),
    then start a new LLM→TTS pipeline."""

    if session.speak_task and not session.speak_task.done():
        session.speak_task.cancel()
        await send_event(session, {"type": "tts.cancel"})

    async def run_turn() -> None:
        session.bot_speaking = True
        try:
            await tts_stream(session, llm_stream(session, text))
            await send_event(session, {"type": "tts.end"})
        finally:
            session.bot_speaking = False

    session.speak_task = asyncio.create_task(run_turn())


async def send_event(session: Session, payload: dict) -> None:
    try:
        await session.ws.send_text(json.dumps(payload))
    except Exception:
        pass


# ─── Duplex WebSocket endpoint ───────────────────────────────

@app.websocket("/voice/sessions/{session_id}")
async def voice_session(ws: WebSocket, session_id: str) -> None:
    """Primary duplex channel between the C# media bot and Python.

    C# streams 20ms PCM frames as {type: "audio.in", pcm: base64}. Python
    streams STT/LLM events and TTS PCM chunks back."""

    await ws.accept()
    session = Session(id=session_id, ws=ws)
    sessions[session_id] = session
    log_event("session.open", session=session_id)
    print(f"[{session_id}] ✓ voice session open")

    stt_task = asyncio.create_task(deepgram_stream(session))

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            kind = msg.get("type")

            if kind == "audio.in":
                pcm_b64 = msg.get("pcm", "")
                if pcm_b64:
                    try:
                        await session.inbound_audio.put(base64.b64decode(pcm_b64))
                    except Exception:
                        pass

            elif kind == "user.start":
                # barge-in: user started talking over the bot
                if session.speak_task and not session.speak_task.done():
                    session.speak_task.cancel()
                    await send_event(session, {"type": "tts.cancel"})

            elif kind == "user.end":
                # best-effort endpoint hint; Deepgram handles endpointing itself
                pass

            elif kind == "ping":
                await ws.send_text(json.dumps({"type": "pong"}))

    except WebSocketDisconnect:
        print(f"[{session_id}] ws disconnected")
    except Exception as exc:
        print(f"[{session_id}] ws error: {exc}")
    finally:
        # signal Deepgram to close cleanly
        await session.inbound_audio.put(b"__END__")
        stt_task.cancel()
        if session.speak_task:
            session.speak_task.cancel()
        sessions.pop(session_id, None)
        log_event("session.close", session=session_id)


# ─── Control plane: tell C# to join / leave ──────────────────

@app.post("/join-meeting")
async def join_meeting(request: Request) -> dict:
    """Instruct C# to join the Teams meeting. C# will open a voice session
    WebSocket back to /voice/sessions/{callId} as soon as the call is live."""

    body = await request.json()
    meeting_url = body.get("meeting_url", "")
    meeting_id = body.get("meeting_id", "")
    passcode = body.get("passcode", "")

    if not meeting_url and not meeting_id:
        return {"error": "meeting_url or meeting_id+passcode is required"}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{CSHARP_URL}/api/join",
                json={"meetingUrl": meeting_url, "meetingId": meeting_id, "passcode": passcode},
            )
            return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/leave-meeting")
async def leave_meeting(request: Request) -> dict:
    body = await request.json()
    call_id = body.get("call_id", "")
    if not call_id:
        return {"error": "call_id is required"}

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(f"{CSHARP_URL}/api/leave", json={"callId": call_id})
            return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


# ─── Lightweight text test (no audio) ────────────────────────

@app.post("/chat")
async def chat(request: Request) -> dict:
    """Non-streaming text test — still useful for verifying LLM config."""
    body = await request.json()
    message = body.get("message", "")
    if not message:
        return {"error": "message is required"}

    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": message}]
    try:
        resp = await llm_client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=messages,
            temperature=0.7,
            max_tokens=200,
        )
        reply = resp.choices[0].message.content or ""
        return {"reply": reply, "model": OPENROUTER_MODEL}
    except Exception as exc:
        return {"error": str(exc)}


# ─── Teams chat bridge (/api/messages) ──────────────────────
# Handles Bot Framework activities delivered by Teams: JWT-authenticates the
# request, extracts the user's text, runs a single-turn LLM call, and replies
# in the same conversation. Uses the same App ID/Secret as the calling bot.

_bot_adapter = BotFrameworkAdapter(
    BotFrameworkAdapterSettings(
        app_id=BOT_APP_ID,
        app_password=BOT_APP_PASSWORD,
        # channel_auth_tenant: empty → MultiTenant; tenant GUID → SingleTenant
        channel_auth_tenant=BOT_TENANT_ID or None,
    )
)

# Pre-trust the well-known Bot Framework service URLs so outbound replies
# pick up the AAD token. The adapter trusts them dynamically after successful
# inbound validation, but doing it up-front avoids a first-request 401.
for _url in [
    "https://webchat.botframework.com/",
    "https://smba.trafficmanager.net/apis/",
    "https://smba.trafficmanager.net/amer/",
    "https://smba.trafficmanager.net/emea/",
    "https://smba.trafficmanager.net/apac/",
    "https://smba.trafficmanager.net/in/",
]:
    MicrosoftAppCredentials.trust_service_url(_url)


class AegisChatBot(ActivityHandler):
    async def on_message_activity(self, ctx: TurnContext) -> None:
        user_text = (ctx.activity.text or "").strip()
        user_name = (ctx.activity.from_property.name if ctx.activity.from_property else "?")

        print(f"[LLM] ▶ message from {user_name}: {user_text!r}")

        if not user_text:
            print("[LLM] ✗ empty text, nothing to send")
            return

        t0 = time.time()
        try:
            print(f"[LLM] → calling {OPENROUTER_MODEL}...")
            resp = await llm_client.chat.completions.create(
                model=OPENROUTER_MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_text},
                ],
                temperature=0.7,
                max_tokens=200,
            )
            reply = resp.choices[0].message.content or "(no reply)"
            elapsed = time.time() - t0
            usage = getattr(resp, "usage", None)
            tokens = f"{usage.total_tokens} tok" if usage else "?"
            print(f"[LLM] ✓ reply ({elapsed:.2f}s, {tokens}): {reply!r}")
        except Exception as exc:
            elapsed = time.time() - t0
            reply = f"Error: {exc}"
            print(f"[LLM] ✗ failed after {elapsed:.2f}s: {type(exc).__name__}: {exc}")

        print(f"[LLM] → sending reply to Teams (conversation={ctx.activity.conversation.id})")
        await ctx.send_activity(reply)
        print("[LLM] ✓ reply delivered")

    async def on_members_added_activity(self, members_added, ctx: TurnContext) -> None:
        for m in members_added:
            if m.id != ctx.activity.recipient.id:
                await ctx.send_activity("Aegis here — ask me anything.")


_chat_bot = AegisChatBot()


@app.post("/api/messages")
async def messages(request: Request) -> Response:
    body = await request.json()
    auth_header = request.headers.get("Authorization", "")

    print(
        f"[/api/messages] in: type={body.get('type')} "
        f"text={body.get('text')!r} "
        f"from={body.get('from', {}).get('name')!r} "
        f"serviceUrl={body.get('serviceUrl')} "
        f"auth_header_present={bool(auth_header)}"
    )

    # Decode the JWT WITHOUT verifying, purely to see its claims.
    if auth_header.lower().startswith("bearer "):
        import jwt as _jwt
        try:
            claims = _jwt.decode(
                auth_header[7:], options={"verify_signature": False, "verify_aud": False}
            )
            print(
                f"[/api/messages] JWT claims: "
                f"aud={claims.get('aud')} "
                f"appid={claims.get('appid')} "
                f"iss={claims.get('iss')} "
                f"tid={claims.get('tid')} "
                f"serviceurl={claims.get('serviceurl')}"
            )
        except Exception as exc:
            print(f"[/api/messages] JWT decode failed: {exc}")

    # Trust this specific serviceUrl so outbound reply is authorised.
    service_url = body.get("serviceUrl")
    if service_url:
        MicrosoftAppCredentials.trust_service_url(service_url)

    activity = Activity().deserialize(body)

    async def turn(ctx: TurnContext) -> None:
        await _chat_bot.on_turn(ctx)

    try:
        invoke_response = await _bot_adapter.process_activity(activity, auth_header, turn)
    except Exception as exc:
        import traceback
        print(f"[/api/messages] error: {type(exc).__name__}: {exc}")
        traceback.print_exc()
        return Response(status_code=500, content=str(exc))

    if invoke_response:
        return Response(
            status_code=invoke_response.status,
            content=json.dumps(invoke_response.body) if invoke_response.body else None,
            media_type="application/json",
        )
    return Response(status_code=201)


# ─── Credential diagnostic ──────────────────────────────────

@app.get("/diag/bot-token")
async def diag_bot_token() -> dict:
    """Try to obtain a Bot Framework AAD token with the configured creds.
    Returns the raw token response (access_token trimmed) so you can see
    exactly why authentication is failing."""
    data = {
        "grant_type": "client_credentials",
        "client_id": BOT_APP_ID,
        "client_secret": BOT_APP_PASSWORD,
        "scope": "https://api.botframework.com/.default",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token",
                data=data,
            )
            body = resp.json()
            if "access_token" in body:
                body["access_token"] = body["access_token"][:20] + "... (truncated)"
            return {"status_code": resp.status_code, "body": body,
                    "app_id_used": BOT_APP_ID[:8] + "...",
                    "secret_len": len(BOT_APP_PASSWORD)}
    except Exception as exc:
        return {"error": str(exc)}


# ─── Batch voice pipeline (HTTP, no WebSocket) ───────────────
# Simple request/response flow for Teams voice:
#   C# captures user audio →
#   C# POSTs /process-audio with audio bytes →
#   Python STT → LLM → TTS (all batch, single-shot) →
#   Python returns TTS audio bytes →
#   C# plays the audio in the Teams meeting via Graph playPrompt.
#
# The streaming /voice/sessions/{id} WebSocket stays in the code for later,
# but it is not used by this flow.

async def _stt_batch(audio_bytes: bytes) -> str:
    """One-shot STT against Deepgram REST (no WebSocket)."""
    if not DEEPGRAM_API_KEY:
        return "[Deepgram not configured]"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://api.deepgram.com/v1/listen"
            "?model=nova-2&language=en-US&punctuate=true&smart_format=true",
            headers={
                "Authorization": f"Token {DEEPGRAM_API_KEY}",
                "Content-Type": "audio/wav",
            },
            content=audio_bytes,
        )
        if resp.status_code != 200:
            print(f"[STT] ✗ {resp.status_code}: {resp.text[:200]}")
            return ""
        data = resp.json()
        transcript = (
            data.get("results", {}).get("channels", [{}])[0]
            .get("alternatives", [{}])[0].get("transcript", "")
        )
        return transcript


async def _llm_batch(user_text: str) -> str:
    """One-shot LLM call (non-streaming)."""
    if not OPENROUTER_API_KEY:
        return f"[LLM not configured] You said: {user_text}"

    resp = await llm_client.chat.completions.create(
        model=OPENROUTER_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        temperature=0.7,
        max_tokens=200,
    )
    return resp.choices[0].message.content or ""


async def _tts_batch(text: str) -> bytes | None:
    """One-shot TTS via ElevenLabs REST (returns MP3 bytes)."""
    if not ELEVENLABS_API_KEY:
        return None

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}",
            headers={
                "xi-api-key": ELEVENLABS_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "text": text,
                "model_id": "eleven_turbo_v2_5",
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.75},
            },
        )
        if resp.status_code != 200:
            print(f"[TTS] ✗ {resp.status_code}: {resp.text[:200]}")
            return None
        return resp.content


@app.post("/process-audio")
async def process_audio(request: Request) -> dict:
    """Full batch voice pipeline. C# posts user audio, gets reply audio back.

    Body:
        { "call_id": "...", "audio_base64": "<base64 WAV>" }

    Returns:
        {
            "transcript": "...",
            "reply": "...",
            "audio_base64": "<base64 MP3>" or null,
            "audio_size": int
        }
    """
    body = await request.json()
    call_id = body.get("call_id", "?")
    audio_b64 = body.get("audio_base64", "")

    if not audio_b64:
        return {"error": "audio_base64 is required"}

    audio_bytes = base64.b64decode(audio_b64)
    print(f"[voice] ▶ /process-audio call={call_id} bytes={len(audio_bytes)}")

    # 1. STT
    t0 = time.time()
    transcript = await _stt_batch(audio_bytes)
    print(f"[voice] 🎤 STT ({time.time()-t0:.2f}s): {transcript!r}")
    if not transcript:
        return {"error": "no transcript", "transcript": ""}

    # 2. LLM
    t0 = time.time()
    reply = await _llm_batch(transcript)
    print(f"[voice] 🤖 LLM ({time.time()-t0:.2f}s): {reply!r}")

    # 3. TTS
    t0 = time.time()
    tts = await _tts_batch(reply)
    print(f"[voice] 🔊 TTS ({time.time()-t0:.2f}s): "
          f"{len(tts) if tts else 0} bytes")

    tts_b64 = base64.b64encode(tts).decode() if tts else None
    print(f"[voice] ✓ round-trip done — returning to C#")

    return {
        "transcript": transcript,
        "reply": reply,
        "audio_base64": tts_b64,
        "audio_size": len(tts) if tts else 0,
    }


# ─── Outbound diagnostic ───────────────────────────────────
# Reproduces what the bot SDK does when replying: get a Bot Framework token,
# then POST a probe to webchat.botframework.com. Shows the raw body so we
# can see WHY the 401 is happening (not just "Unauthorized").

@app.get("/diag/outbound-probe")
async def diag_outbound_probe() -> dict:
    """Try both Multi-Tenant and Single-Tenant token endpoints and probe
    Bot Framework. Whichever combination returns 200/201 from BF is the
    correct adapter configuration for this bot."""

    async def _probe(token_url: str, label: str) -> dict:
        async with httpx.AsyncClient(timeout=15.0) as client:
            token_resp = await client.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": BOT_APP_ID,
                    "client_secret": BOT_APP_PASSWORD,
                    "scope": "https://api.botframework.com/.default",
                },
            )
            if token_resp.status_code != 200:
                return {"label": label, "stage": "token",
                        "status": token_resp.status_code,
                        "body": token_resp.json()}

            token = token_resp.json()["access_token"]
            probe = await client.post(
                "https://webchat.botframework.com/v3/conversations",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"bot": {"id": BOT_APP_ID}, "members": [{"id": "probe-user"}]},
            )
            return {
                "label": label,
                "stage": "probe",
                "status_code": probe.status_code,
                "response_body": probe.text[:400],
            }

    multi_tenant_url = "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token"
    single_tenant_url = (
        f"https://login.microsoftonline.com/{BOT_TENANT_ID}/oauth2/v2.0/token"
        if BOT_TENANT_ID else None
    )

    results = {
        "app_id_used": BOT_APP_ID,
        "tenant_id": BOT_TENANT_ID or "(not set)",
        "multi_tenant": await _probe(multi_tenant_url, "MultiTenant"),
    }
    if single_tenant_url:
        results["single_tenant"] = await _probe(single_tenant_url, "SingleTenant")
    return results


# ─── Control plane (C# → Python notifications) ──────────────

@app.post("/control")
async def control(request: Request) -> dict:
    """Best-effort notifications from C#: call_connected, call_established,
    call_disconnected, incoming_call, etc. The live audio path is the
    WebSocket at /voice/sessions/{id} — this is only for lifecycle events."""
    body = await request.json()
    kind = body.get("type", "unknown")
    data = body.get("data", {})
    log_event("control." + kind, data=data)
    print(f"[control] {kind}: {data}")
    return {"status": "ok"}


# ─── Health / status ─────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    csharp_ok = False
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{CSHARP_URL}/health")
            csharp_ok = resp.status_code == 200
    except Exception:
        pass

    return {
        "service": "aegis-python-backend",
        "status": "ok",
        "csharp_service": "connected" if csharp_ok else "unreachable",
        "csharp_url": CSHARP_URL,
        "active_sessions": len(sessions),
        "keys_configured": {
            "openrouter": bool(OPENROUTER_API_KEY),
            "deepgram": bool(DEEPGRAM_API_KEY),
            "elevenlabs": bool(ELEVENLABS_API_KEY),
        },
    }


@app.get("/status")
async def status() -> dict:
    return {
        "sessions": [
            {"id": s.id, "bot_speaking": s.bot_speaking, "history_len": len(s.history)}
            for s in sessions.values()
        ],
        "recent_events": list(event_log)[-20:],
    }
