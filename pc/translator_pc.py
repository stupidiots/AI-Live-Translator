# AI Live Translator — PC (Windows) version
# Mirrors the Android v1.9 feature set:
#   * Gemini Live Translate (BYOK — each user brings their own API key)
#   * Two capture modes: system audio (WASAPI loopback) and microphone
#   * Dual subtitles (original + translation), floating subtitle overlay
#   * Adsterra Smartlink ad gate on every Start (5 s countdown, fail-open)
#   * Facebook follow button, My/En UI toggle, dub volume slider
#   * Session resumption + auto-reconnect, remote version check (fail-open)
#
# Protocol follows the proven Android implementation: ephemeral token mint
# (POST /v1beta/auth_tokens) -> WebSocket BidiGenerateContent with the token,
# one-time fallback to direct key auth if the token socket dies before setup.
import asyncio
import base64
import json
import os
import queue
import sys
import threading
import time
import urllib.request
import urllib.parse
import webbrowser
from datetime import datetime, timedelta, timezone

import numpy as np

try:
    import pyaudiowpatch as pyaudio
except ImportError:  # plain PyAudio has no WASAPI loopback; mic still works
    import pyaudio

import websockets

APP_NAME = "AI Live Translator"
PC_VERSION_CODE = 1
PC_VERSION_NAME = "1.0"
MODEL = "gemini-3.5-live-translate-preview"
WS_URL = ("wss://generativelanguage.googleapis.com/ws/"
          "google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent")
TOKEN_URL = "https://generativelanguage.googleapis.com/v1beta/auth_tokens"
VERSION_URL = ("https://raw.githubusercontent.com/stupidiots/"
               "AI-Live-Translator/main/pc_version.json")
SMARTLINK = "https://asiafilm.org/4/4ddc8d335c6aa45d829f3922abeec120"
FACEBOOK_URL = "https://www.facebook.com/Airdropgod"
TELEGRAM_URL = "https://t.me/AirdropVsTermux"

SAMPLE_RATE_IN = 16000          # Gemini Live input: 16 kHz PCM16 mono
SAMPLE_RATE_OUT = 24000         # Gemini Live output: 24 kHz PCM16 mono
CHUNK_SAMPLES = 1600            # 100 ms per realtimeInput message

VOICES = ["Puck", "Charon", "Kore", "Fenrir", "Aoede", "Leda", "Orus", "Zephyr"]

LANGUAGES = [
    ("my", "Myanmar"), ("en", "English"), ("th", "Thai"), ("zh", "Chinese"),
    ("ja", "Japanese"), ("ko", "Korean"), ("hi", "Hindi"), ("vi", "Vietnamese"),
    ("id", "Indonesian"), ("ms", "Malay"), ("km", "Khmer"), ("lo", "Lao"),
    ("bn", "Bengali"), ("ta", "Tamil"), ("te", "Telugu"), ("ur", "Urdu"),
    ("ar", "Arabic"), ("fa", "Persian"), ("tr", "Turkish"), ("ru", "Russian"),
    ("es", "Spanish"), ("fr", "French"), ("de", "German"), ("it", "Italian"),
    ("pt", "Portuguese"),
]

STR = {
    "en": {
        "title": "AI Live Translator",
        "api_key": "Gemini API key",
        "key_hint": "Your own key from Google AI Studio. Saved only on this PC.",
        "mode": "Audio source",
        "system_audio": "System audio (PC sound)",
        "mic": "Microphone",
        "target": "Translate to",
        "voice": "Voice",
        "volume": "Dub volume",
        "dub_device": "Dub plays on",
        "default_device": "Default output",
        "echo_hint": "Tip: if the dub plays on the same speakers that System audio captures, use headphones or pick a different dub output to avoid echo.",
        "start": "Start",
        "stop": "Stop",
        "original": "Original",
        "translation": "Translation",
        "widget": "Subtitle widget",
        "follow": "Follow on Facebook",
        "join_tg": "Join Telegram",
        "ready": "Ready",
        "connecting": "Connecting...",
        "live": "Live — translating",
        "stopped": "Stopped",
        "reconnecting": "Connection lost — reconnecting...",
        "err_no_key": "Enter your Gemini API key first.",
        "err_prefix": "Error: ",
        "ad_title": "Advertisement",
        "ad_body": "An ad page opens in your browser. You can close this and start translating in",
        "ad_seconds": "seconds",
        "ad_close": "Close",
        "update_title": "Update available",
        "update_body": "A new version of AI Live Translator is available. Please update to keep translating.",
        "blocked_body": "This version is no longer supported. Please update to keep translating.",
        "update_now": "Update now",
        "later": "Later",
    },
    "my": {
        "title": "AI Live ဘာသာပြန်",
        "api_key": "Gemini API key",
        "key_hint": "Google AI Studio က ရတဲ့ ကိုယ်ပိုင် key ထည့်ပါ။ ဒီ PC ပေါ်မှာပဲ သိမ်းထားပါတယ်။",
        "mode": "အသံ အရင်းအမြစ်",
        "system_audio": "System audio (PC အသံ)",
        "mic": "မိုက်ခရိုဖုန်း",
        "target": "ဘာသာပြန်မည့် ဘာသာ",
        "voice": "အသံ",
        "volume": "ဘာသာပြန်အသံ အတိုးအကျယ်",
        "dub_device": "ဘာသာပြန်အသံ ထွက်မည့် device",
        "default_device": "Default output",
        "echo_hint": "အကြံပြုချက်: System audio ဖမ်းတဲ့ speaker ကပဲ ဘာသာပြန်အသံ ပြန်ထွက်နေရင် echo ဝင်နိုင်တယ် — နားကြပ်သုံးပါ၊ ဒါမှမဟုတ် dub output device ပြောင်းပါ။",
        "start": "စတင်",
        "stop": "ရပ်",
        "original": "မူရင်း",
        "translation": "ဘာသာပြန်",
        "widget": "စာတန်း widget",
        "follow": "Facebook မှာ Follow လုပ်ပါ",
        "join_tg": "Telegram မှာ Join ပါ",
        "ready": "အဆင်သင့်",
        "connecting": "ချိတ်ဆက်နေသည်...",
        "live": "တိုက်ရိုက် — ဘာသာပြန်နေသည်",
        "stopped": "ရပ်ထားသည်",
        "reconnecting": "ချိတ်ဆက်မှု ပြတ်သွားသည် — ပြန်ချိတ်နေသည်...",
        "err_no_key": "Gemini API key အရင် ထည့်ပါ။",
        "err_prefix": "အမှား: ",
        "ad_title": "ကြော်ငြာ",
        "ad_body": "Browser မှာ ကြော်ငြာ page ပွင့်ပါမယ်။ ဒါကို ပိတ်ပြီး ဘာသာပြန် စတင်နိုင်မည့်အချိန်",
        "ad_seconds": "စက္ကန့်",
        "ad_close": "ပိတ်မယ်",
        "update_title": "Version အသစ် ရပါပြီ",
        "update_body": "AI Live Translator ဗားရှင်းအသစ် ရပါပြီ။ ဆက်လက် အသုံးပြုနိုင်ဖို့ update လုပ်ပေးပါ။",
        "blocked_body": "ဒီ version ကို ဆက် အသုံးပြု၍ မရတော့ပါ။ update လုပ်ပေးပါ။",
        "update_now": "Update လုပ်မယ်",
        "later": "နောက်မှ",
    },
}


def settings_path():
    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    folder = os.path.join(base, "AILiveTranslator")
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, "settings.json")


class Settings:
    def __init__(self):
        self.data = {
            "api_key": "", "mode": "system", "target": "my", "voice": "Puck",
            "volume": 100, "ui_lang": "my", "dub_device": -1,
        }
        try:
            with open(settings_path(), "r", encoding="utf-8") as f:
                self.data.update(json.load(f))
        except Exception:
            pass

    def save(self):
        try:
            with open(settings_path(), "w", encoding="utf-8") as f:
                json.dump(self.data, f)
        except Exception:
            pass

    def __getitem__(self, k):
        return self.data[k]

    def __setitem__(self, k, v):
        self.data[k] = v


class Resampler:
    """Stateful linear-interpolation resampler to 16 kHz mono float32."""

    def __init__(self, src_rate):
        self.ratio = float(src_rate) / SAMPLE_RATE_IN
        self.leftover = np.zeros(0, dtype=np.float32)
        self.phase = 0.0

    def process(self, mono_f32):
        if len(mono_f32) == 0:
            return np.zeros(0, dtype=np.float32)
        data = np.concatenate([self.leftover, mono_f32])
        out_len = int((len(data) - 1 - self.phase) / self.ratio) + 1
        if out_len <= 0:
            self.leftover = data
            return np.zeros(0, dtype=np.float32)
        idx = self.phase + np.arange(out_len) * self.ratio
        last = int(idx[-1])
        out = np.interp(idx, np.arange(len(data)), data).astype(np.float32)
        consumed = last + 1
        self.leftover = data[consumed - 1:] if consumed >= 1 else data
        self.phase = (self.phase + out_len * self.ratio) - consumed
        if self.phase < 0:
            self.phase = 0.0
        return out


def _iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{dt.microsecond // 1000:03d}Z"


class GeminiClient:
    """asyncio port of the Android GeminiLiveClient (token auth + one-time
    key fallback, session resumption, goAway resume, pending-audio queue).
    Callbacks fire on the asyncio thread; the GUI marshals via a queue."""

    def __init__(self, events):
        self.events = events            # queue.Queue of (kind, payload)
        self.loop = None
        self.ws = None
        self.api_key = ""
        self.voice = "Puck"
        self.target_code = "my"
        self.token = None
        self.resumption_handle = None
        self.setup_done = False
        self.closed_by_user = False
        self.used_key_fallback = False
        self.initial_connect = False
        self.became_ready = False
        self.resume_tried_since_ready = False
        self.pending_audio = []
        self._send_lock = None

    # ---------- public API (called from GUI thread) ----------
    def start(self, api_key, voice, target_code):
        self.api_key = api_key
        self.voice = voice
        self.target_code = target_code
        self.closed_by_user = False
        self.resumption_handle = None
        self.token = None
        self.used_key_fallback = False
        self.initial_connect = True
        self.became_ready = False
        self._run(self._connect_fresh())

    def stop(self):
        self.closed_by_user = True
        self.setup_done = False
        self.pending_audio = []
        ws = self.ws
        if ws is not None and self.loop is not None:
            asyncio.run_coroutine_threadsafe(self._close_ws(ws), self.loop)

    def send_audio(self, pcm_bytes):
        b64 = base64.b64encode(pcm_bytes).decode("ascii")
        if not self.setup_done:
            self.pending_audio.append(b64)
            del self.pending_audio[:-10]
            return
        self._run(self._send_audio_now(b64))

    def reconnect(self):
        self.closed_by_user = False
        old, self.ws = self.ws, None
        if old is not None and self.loop is not None:
            asyncio.run_coroutine_threadsafe(self._close_ws(old), self.loop)
        self.setup_done = False
        if self.resumption_handle and not self.resume_tried_since_ready:
            self._run(self._resume())
        else:
            self.start(self.api_key, self.voice, self.target_code)

    # ---------- internals ----------
    def _run(self, coro):
        if self.loop is not None:
            asyncio.run_coroutine_threadsafe(coro, self.loop)

    async def _close_ws(self, ws):
        try:
            await ws.close()
        except Exception:
            pass

    def attach_loop(self, loop):
        self.loop = loop
        self._send_lock = asyncio.Lock()

    async def _mint_token(self):
        now = datetime.now(timezone.utc)
        body = json.dumps({
            "uses": 1,
            "expireTime": _iso(now + timedelta(minutes=30)),
            "newSessionExpireTime": _iso(now + timedelta(minutes=2)),
        }).encode("utf-8")

        def _post():
            req = urllib.request.Request(TOKEN_URL, data=body, method="POST")
            req.add_header("x-goog-api-key", self.api_key)
            req.add_header("Content-Type", "application/json")
            with urllib.request.urlopen(req, timeout=25) as resp:
                return resp.status, resp.read().decode("utf-8", "replace")

        try:
            status, text = await asyncio.to_thread(_post)
        except urllib.error.HTTPError as e:
            text = e.read().decode("utf-8", "replace") if e.fp else ""
            if e.code in (400, 401, 403):
                self.events.put(("auth_error", f"HTTP {e.code}: {text[:200]}"))
            else:
                self.events.put(("error", f"HTTP {e.code}: {text[:200]}"))
            return None
        except Exception as e:
            if not self.closed_by_user:
                self.events.put(("error", f"token request failed: {e}"))
            return None
        try:
            name = json.loads(text).get("name", "")
        except Exception:
            name = ""
        if not name:
            self.events.put(("error", f"bad token response: {text[:200]}"))
            return None
        return name

    async def _connect_fresh(self):
        token = await self._mint_token()
        if token is None or self.closed_by_user:
            return
        self.token = token
        url = WS_URL + "?access_token=" + urllib.parse.quote(token, safe="")
        await self._open_socket(url, mode="token")

    async def _resume(self):
        self.resume_tried_since_ready = True
        if self.token:
            url = WS_URL + "?access_token=" + urllib.parse.quote(self.token, safe="")
            await self._open_socket(url, mode="token")
        else:
            await self._connect_fresh()

    async def _open_socket(self, url, mode):
        self.became_ready = False
        try:
            ws = await websockets.connect(url, ping_interval=20, ping_timeout=20,
                                          max_size=None)
        except Exception as e:
            if self.closed_by_user:
                return
            if mode == "token" and self._try_fallback():
                return
            status = getattr(e, "status", None) or getattr(e, "status_code", None)
            if status in (401, 403):
                self.events.put(("auth_error", str(e)))
            else:
                self.events.put(("error", str(e)))
            return
        self.ws = ws
        await self._send_setup()
        try:
            async for raw in ws:
                if ws is not self.ws:
                    break
                self._handle(raw)
        except Exception:
            pass
        finally:
            self.setup_done = False
            if not self.closed_by_user and ws is self.ws:
                if self._try_fallback():
                    return
                self.events.put(("closed", ""))

    def _try_fallback(self):
        """One automatic retry with API-key auth when the token connection
        dies before setupComplete (mirrors the Android client)."""
        if (self.closed_by_user or not self.initial_connect or
                self.used_key_fallback or self.became_ready):
            return False
        self.used_key_fallback = True
        url = WS_URL + "?key=" + urllib.parse.quote(self.api_key, safe="")
        self._run(self._open_socket(url, mode="key"))
        return True

    async def _send_setup(self):
        gen = {
            "responseModalities": ["AUDIO"],
            "speechConfig": {"voiceConfig": {"prebuiltVoiceConfig":
                                             {"voiceName": self.voice}}},
            "translationConfig": {"targetLanguageCode": self.target_code,
                                  "echoTargetLanguage": True},
        }
        setup = {
            "model": "models/" + MODEL,
            "generationConfig": gen,
            "inputAudioTranscription": {},
            "outputAudioTranscription": {},
            "sessionResumption": ({"handle": self.resumption_handle}
                                  if self.resumption_handle else {}),
        }
        await self.ws.send(json.dumps({"setup": setup}))

    async def _send_audio_now(self, b64):
        ws = self.ws
        if ws is None:
            return
        payload = {"realtimeInput": {"audio": {
            "mimeType": "audio/pcm;rate=16000", "data": b64}}}
        try:
            async with self._send_lock:
                await ws.send(json.dumps(payload))
        except Exception:
            pass

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if "setupComplete" in msg:
            self.setup_done = True
            self.became_ready = True
            self.initial_connect = False
            self.resume_tried_since_ready = False
            pending, self.pending_audio = self.pending_audio, []
            for b64 in pending:
                self._run(self._send_audio_now(b64))
            self.events.put(("ready", ""))
        elif "sessionResumptionUpdate" in msg:
            h = (msg.get("sessionResumptionUpdate") or {}).get("newHandle", "")
            if h:
                self.resumption_handle = h
        elif "goAway" in msg:
            self.events.put(("goaway", ""))
        elif "serverContent" in msg:
            sc = msg.get("serverContent") or {}
            if sc.get("interrupted"):
                self.events.put(("interrupted", ""))
            it = (sc.get("inputTranscription") or {}).get("text", "")
            if it:
                self.events.put(("original", it))
            ot = (sc.get("outputTranscription") or {}).get("text", "")
            if ot:
                self.events.put(("translated", ot))
            parts = ((sc.get("modelTurn") or {}).get("parts")) or []
            for part in parts:
                data = (part.get("inlineData") or {}).get("data", "")
                if data:
                    try:
                        self.events.put(("audio", base64.b64decode(data)))
                    except Exception:
                        pass
            if sc.get("turnComplete"):
                self.events.put(("turn", ""))
        elif "error" in msg:
            err = msg.get("error") or {}
            detail = f"({err.get('code', '')}) {err.get('message', '')}".strip()
            if not self.setup_done and self._try_fallback():
                return
            if "API key" in detail or "API_KEY" in detail:
                self.events.put(("auth_error", detail))
            else:
                self.events.put(("error", detail))


class AudioEngine:
    """WASAPI loopback (system audio) or microphone capture -> 16 kHz mono
    PCM chunks; dub playback at 24 kHz with software volume."""

    def __init__(self, on_chunk, on_error):
        self.on_chunk = on_chunk        # called with bytes (100 ms, 16 kHz)
        self.on_error = on_error
        self.pa = None
        self.in_stream = None
        self.out_stream = None
        self.capture_thread = None
        self.play_thread = None
        self.running = False
        self.play_queue = queue.Queue()
        self.volume = 1.0

    # ---------- device helpers ----------
    def list_output_devices(self):
        """[(index, name)] for WASAPI output devices (dub picker)."""
        out = []
        try:
            pa = pyaudio.PyAudio()
            wasapi = None
            for i in range(pa.get_host_api_count()):
                if pa.get_host_api_info_by_index(i)["name"] == "Windows WASAPI":
                    wasapi = i
                    break
            for i in range(pa.get_device_count()):
                d = pa.get_device_info_by_index(i)
                if d["maxOutputChannels"] > 0 and (wasapi is None or
                                                   d["hostApi"] == wasapi):
                    out.append((i, d["name"]))
            pa.terminate()
        except Exception:
            pass
        return out

    def _open_input(self, mode):
        if mode == "mic":
            dev = self.pa.get_default_input_device_info()
            rate = int(dev["defaultSampleRate"])
            channels = min(2, int(dev["maxInputChannels"])) or 1
            stream = self.pa.open(format=pyaudio.paFloat32, channels=channels,
                                  rate=rate, input=True,
                                  input_device_index=int(dev["index"]),
                                  frames_per_buffer=1024)
            return stream, rate, channels
        # system audio: WASAPI loopback of the default output device
        try:
            dev = self.pa.get_default_wasapi_loopback()
        except Exception:
            dev = None
        if dev is None:
            for d in self.pa.get_loopback_device_info_generator():
                dev = d
                break
        if dev is None:
            raise RuntimeError("No WASAPI loopback device found")
        rate = int(dev["defaultSampleRate"])
        channels = int(dev["maxInputChannels"]) or 2
        stream = self.pa.open(format=pyaudio.paFloat32, channels=channels,
                              rate=rate, input=True,
                              input_device_index=int(dev["index"]),
                              frames_per_buffer=1024)
        return stream, rate, channels

    # ---------- lifecycle ----------
    def start(self, mode, dub_device_index=-1):
        self.stop()
        self.pa = pyaudio.PyAudio()
        self.in_stream, rate, channels = self._open_input(mode)
        self.out_stream = self.pa.open(
            format=pyaudio.paInt16, channels=1, rate=SAMPLE_RATE_OUT,
            output=True,
            output_device_index=(None if dub_device_index is None or
                                 dub_device_index < 0 else dub_device_index),
            frames_per_buffer=1024)
        self.running = True
        self.capture_thread = threading.Thread(
            target=self._capture_loop, args=(rate, channels), daemon=True)
        self.capture_thread.start()
        self.play_thread = threading.Thread(target=self._play_loop, daemon=True)
        self.play_thread.start()

    def stop(self):
        self.running = False
        for s in (self.in_stream, self.out_stream):
            if s is not None:
                try:
                    s.stop_stream()
                    s.close()
                except Exception:
                    pass
        self.in_stream = None
        self.out_stream = None
        if self.pa is not None:
            try:
                self.pa.terminate()
            except Exception:
                pass
            self.pa = None
        self.flush_dub()

    def flush_dub(self):
        try:
            while True:
                self.play_queue.get_nowait()
        except queue.Empty:
            pass

    def play_dub(self, pcm_bytes):
        self.play_queue.put(pcm_bytes)

    # ---------- workers ----------
    def _capture_loop(self, rate, channels):
        resampler = Resampler(rate)
        acc = np.zeros(0, dtype=np.int16)
        while self.running:
            try:
                raw = self.in_stream.read(1024, exception_on_overflow=False)
            except Exception as e:
                if self.running:
                    self.on_error(f"capture: {e}")
                break
            f32 = np.frombuffer(raw, dtype=np.float32)
            if channels > 1:
                f32 = f32.reshape(-1, channels).mean(axis=1)
            out = resampler.process(f32.astype(np.float32))
            if len(out) == 0:
                continue
            pcm = (np.clip(out, -1.0, 1.0) * 32767).astype(np.int16)
            acc = np.concatenate([acc, pcm])
            while len(acc) >= CHUNK_SAMPLES:
                self.on_chunk(acc[:CHUNK_SAMPLES].tobytes())
                acc = acc[CHUNK_SAMPLES:]

    def _play_loop(self):
        while self.running:
            try:
                data = self.play_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if self.out_stream is None:
                continue
            try:
                if self.volume != 1.0:
                    arr = np.frombuffer(data, dtype=np.int16).astype(np.float32)
                    arr = np.clip(arr * self.volume, -32768, 32767)
                    data = arr.astype(np.int16).tobytes()
                self.out_stream.write(data)
            except Exception:
                pass


import tkinter as tk
from tkinter import ttk

BG = "#0A0F1E"
PANEL = "#111A30"
ACCENT = "#22D3EE"
ACCENT2 = "#818CF8"
TEXT = "#E5EAF5"
MUTED = "#8A94A8"


class App:
    def __init__(self, root):
        self.root = root
        self.settings = Settings()
        self.events = queue.Queue()
        self.client = GeminiClient(self.events)
        self.loop = asyncio.new_event_loop()
        self.client.attach_loop(self.loop)
        threading.Thread(target=self.loop.run_forever, daemon=True).start()
        self.engine = AudioEngine(
            on_chunk=lambda b: self.client.send_audio(b),
            on_error=lambda m: self.events.put(("error", m)))
        self.session_active = False
        self.is_ready = False
        self.reconnects = 0
        self.version_blocked = False
        self.update_url = ""
        self.orig_line = ""
        self.tran_line = ""
        self.overlay = None
        self.overlay_tran = None
        self.overlay_orig = None
        self._build_ui()
        self._apply_lang()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(60, self._pump)
        threading.Thread(target=self._version_check, daemon=True).start()

    # ---------------- UI construction ----------------
    def _build_ui(self):
        s = self.settings
        self.root.title(APP_NAME)
        self.root.configure(bg=BG)
        self.root.geometry("540x680")
        top = tk.Frame(self.root, bg=BG)
        top.pack(fill="x", padx=14, pady=(12, 0))
        self.title_lbl = tk.Label(top, bg=BG, fg=TEXT,
                                  font=("Segoe UI", 16, "bold"))
        self.title_lbl.pack(side="left")
        self.lang_btn = tk.Button(top, bg=PANEL, fg=ACCENT, bd=0, padx=10,
                                  pady=4, command=self._toggle_lang)
        self.lang_btn.pack(side="right")
        self.follow_btn = tk.Button(top, bg=PANEL, fg=ACCENT2, bd=0, padx=10,
                                    pady=4, command=lambda:
                                    webbrowser.open(FACEBOOK_URL))
        self.follow_btn.pack(side="right", padx=(0, 8))
        self.tg_btn = tk.Button(top, bg=PANEL, fg=ACCENT, bd=0, padx=10,
                                pady=4, command=lambda:
                                webbrowser.open(TELEGRAM_URL))
        self.tg_btn.pack(side="right", padx=(0, 8))

        body = tk.Frame(self.root, bg=BG)
        body.pack(fill="both", expand=True, padx=14, pady=10)

        self.key_lbl = tk.Label(body, bg=BG, fg=MUTED, anchor="w")
        self.key_lbl.pack(fill="x")
        self.key_var = tk.StringVar(value=s["api_key"])
        tk.Entry(body, textvariable=self.key_var, show="*", bg=PANEL, fg=TEXT,
                 insertbackground=TEXT, bd=0).pack(fill="x", ipady=5, pady=(2, 2))
        self.key_hint = tk.Label(body, bg=BG, fg=MUTED, anchor="w",
                                 font=("Segoe UI", 8))
        self.key_hint.pack(fill="x", pady=(0, 8))

        self.mode_lbl = tk.Label(body, bg=BG, fg=MUTED, anchor="w")
        self.mode_lbl.pack(fill="x")
        self.mode_var = tk.StringVar(value=s["mode"])
        mrow = tk.Frame(body, bg=BG)
        mrow.pack(fill="x", pady=(2, 8))
        self.mode_sys = tk.Radiobutton(mrow, variable=self.mode_var,
                                       value="system", bg=BG, fg=TEXT,
                                       selectcolor=PANEL, activebackground=BG,
                                       activeforeground=TEXT)
        self.mode_sys.pack(side="left")
        self.mode_mic = tk.Radiobutton(mrow, variable=self.mode_var,
                                       value="mic", bg=BG, fg=TEXT,
                                       selectcolor=PANEL, activebackground=BG,
                                       activeforeground=TEXT)
        self.mode_mic.pack(side="left", padx=(12, 0))

        row = tk.Frame(body, bg=BG)
        row.pack(fill="x", pady=(0, 8))
        left = tk.Frame(row, bg=BG)
        left.pack(side="left", fill="x", expand=True)
        self.target_lbl = tk.Label(left, bg=BG, fg=MUTED, anchor="w")
        self.target_lbl.pack(fill="x")
        self.target_var = tk.StringVar(value=dict(LANGUAGES)[s["target"]]
                                       if s["target"] in dict(LANGUAGES)
                                       else "Myanmar")
        ttk.Combobox(left, textvariable=self.target_var, state="readonly",
                     values=[n for _, n in LANGUAGES]).pack(fill="x")
        right = tk.Frame(row, bg=BG)
        right.pack(side="left", fill="x", expand=True, padx=(10, 0))
        self.voice_lbl = tk.Label(right, bg=BG, fg=MUTED, anchor="w")
        self.voice_lbl.pack(fill="x")
        self.voice_var = tk.StringVar(value=s["voice"])
        ttk.Combobox(right, textvariable=self.voice_var, state="readonly",
                     values=VOICES).pack(fill="x")

        self.dub_lbl = tk.Label(body, bg=BG, fg=MUTED, anchor="w")
        self.dub_lbl.pack(fill="x")
        self.dub_devices = self.engine.list_output_devices()
        self.dub_var = tk.StringVar()
        dub_names = ["Default output"] + [n for _, n in self.dub_devices]
        self.dub_combo = ttk.Combobox(body, textvariable=self.dub_var,
                                      state="readonly", values=dub_names)
        self.dub_combo.pack(fill="x", pady=(2, 2))
        self.dub_var.set(dub_names[0])
        for idx, name in self.dub_devices:
            if idx == s["dub_device"]:
                self.dub_var.set(name)
        self.echo_hint = tk.Label(body, bg=BG, fg=MUTED, anchor="w",
                                  font=("Segoe UI", 8), wraplength=505,
                                  justify="left")
        self.echo_hint.pack(fill="x", pady=(0, 8))

        self.vol_lbl = tk.Label(body, bg=BG, fg=MUTED, anchor="w")
        self.vol_lbl.pack(fill="x")
        self.vol_var = tk.IntVar(value=int(s["volume"]))
        tk.Scale(body, variable=self.vol_var, from_=0, to=200,
                 orient="horizontal", bg=BG, fg=TEXT, troughcolor=PANEL,
                 highlightthickness=0, bd=0,
                 command=self._on_volume).pack(fill="x", pady=(0, 8))

        ctl = tk.Frame(body, bg=BG)
        ctl.pack(fill="x", pady=(0, 8))
        self.start_btn = tk.Button(ctl, bg=ACCENT, fg="#06222B", bd=0,
                                   font=("Segoe UI", 12, "bold"), padx=22,
                                   pady=7, command=self._on_start_stop)
        self.start_btn.pack(side="left")
        self.widget_btn = tk.Button(ctl, bg=PANEL, fg=TEXT, bd=0, padx=12,
                                    pady=7, command=self._toggle_overlay)
        self.widget_btn.pack(side="left", padx=(10, 0))
        self.status_lbl = tk.Label(body, bg=BG, fg=ACCENT, anchor="w")
        self.status_lbl.pack(fill="x", pady=(0, 6))

        self.orig_lbl = tk.Label(body, bg=BG, fg=MUTED, anchor="w")
        self.orig_lbl.pack(fill="x")
        self.orig_text = tk.Text(body, height=6, bg=PANEL, fg=MUTED, bd=0,
                                 wrap="word", font=("Segoe UI", 11))
        self.orig_text.pack(fill="x", pady=(2, 8))
        self.tran_lbl = tk.Label(body, bg=BG, fg=MUTED, anchor="w")
        self.tran_lbl.pack(fill="x")
        self.tran_text = tk.Text(body, height=9, bg=PANEL, fg=TEXT, bd=0,
                                 wrap="word", font=("Segoe UI", 13))
        self.tran_text.pack(fill="both", expand=True, pady=(2, 0))

    def _apply_lang(self):
        t = STR[self.settings["ui_lang"]]
        self.title_lbl.config(text=t["title"])
        self.lang_btn.config(text="EN" if self.settings["ui_lang"] == "my"
                             else "မြန်မာ")
        self.follow_btn.config(text=t["follow"])
        self.tg_btn.config(text=t["join_tg"])
        self.key_lbl.config(text=t["api_key"])
        self.key_hint.config(text=t["key_hint"])
        self.mode_lbl.config(text=t["mode"])
        self.mode_sys.config(text=t["system_audio"])
        self.mode_mic.config(text=t["mic"])
        self.target_lbl.config(text=t["target"])
        self.voice_lbl.config(text=t["voice"])
        self.dub_lbl.config(text=t["dub_device"])
        self.echo_hint.config(text=t["echo_hint"])
        self.vol_lbl.config(text=t["volume"])
        self.start_btn.config(text=t["stop"] if self.session_active
                              else t["start"])
        self.widget_btn.config(text=t["widget"])
        self.orig_lbl.config(text=t["original"])
        self.tran_lbl.config(text=t["translation"])
        if not self.session_active:
            self.status_lbl.config(text=t["ready"])

    def _toggle_lang(self):
        self.settings["ui_lang"] = ("en" if self.settings["ui_lang"] == "my"
                                    else "my")
        self.settings.save()
        self._apply_lang()

    def _on_volume(self, _v=None):
        v = int(self.vol_var.get())
        self.engine.volume = v / 100.0
        self.settings["volume"] = v
        self.settings.save()

    # ---------------- session flow ----------------
    def _on_start_stop(self):
        if self.session_active:
            self._stop_session()
        else:
            self._begin_start()

    def _begin_start(self):
        t = STR[self.settings["ui_lang"]]
        if self.version_blocked:
            self._show_update_dialog(blocked=True)
            return
        if not self.key_var.get().strip():
            self.status_lbl.config(text=t["err_no_key"], fg="#F87171")
            return
        self.status_lbl.config(fg=ACCENT)
        self._ad_gate(self._start_session)

    def _start_session(self):
        t = STR[self.settings["ui_lang"]]
        s = self.settings
        s["api_key"] = self.key_var.get().strip()
        s["mode"] = self.mode_var.get()
        name_to_code = {n: c for c, n in LANGUAGES}
        s["target"] = name_to_code.get(self.target_var.get(), "my")
        s["voice"] = self.voice_var.get()
        dub_index = -1
        for idx, name in self.dub_devices:
            if name == self.dub_var.get():
                dub_index = idx
        s["dub_device"] = dub_index
        s.save()
        self.engine.volume = int(s["volume"]) / 100.0
        self.status_lbl.config(text=t["connecting"])
        try:
            self.engine.start(s["mode"], dub_index)
        except Exception as e:
            self.status_lbl.config(text=t["err_prefix"] + str(e), fg="#F87171")
            return
        self.session_active = True
        self.is_ready = False
        self.reconnects = 0
        self.start_btn.config(text=t["stop"])
        self.client.start(s["api_key"], s["voice"], s["target"])

    def _stop_session(self):
        t = STR[self.settings["ui_lang"]]
        self.session_active = False
        self.is_ready = False
        self.client.stop()
        self.engine.stop()
        self.start_btn.config(text=t["start"])
        self.status_lbl.config(text=t["stopped"], fg=ACCENT)

    # ---------------- ad gate ----------------
    def _ad_gate(self, proceed):
        t = STR[self.settings["ui_lang"]]
        win = tk.Toplevel(self.root)
        win.title(t["ad_title"])
        win.configure(bg=BG)
        win.geometry("360x170")
        win.transient(self.root)
        win.grab_set()
        try:
            webbrowser.open(SMARTLINK)
        except Exception:
            pass  # fail-open: the session must never depend on the ad
        lbl = tk.Label(win, bg=BG, fg=TEXT, wraplength=330, justify="center",
                       font=("Segoe UI", 11))
        lbl.pack(padx=16, pady=(18, 8))
        btn = tk.Button(win, bg=PANEL, fg=MUTED, bd=0, padx=16, pady=6,
                        state="disabled", text=t["ad_close"])
        btn.pack(pady=(4, 0))
        state = {"left": 5, "done": False}

        def tick():
            if state["done"]:
                return
            if state["left"] <= 0:
                lbl.config(text=t["ad_body"] + " 0 " + t["ad_seconds"])
                btn.config(state="normal", fg=TEXT)
                return
            lbl.config(text=f"{t['ad_body']} {state['left']} "
                            f"{t['ad_seconds']}")
            state["left"] -= 1
            win.after(1000, tick)

        def close():
            if state["left"] > 0:
                return
            state["done"] = True
            win.destroy()
            proceed()

        btn.config(command=close)
        win.protocol("WM_DELETE_WINDOW",
                     lambda: close() if state["left"] <= 0 else None)
        tick()

    # ---------------- version check ----------------
    def _version_check(self):
        try:
            req = urllib.request.Request(VERSION_URL)
            with urllib.request.urlopen(req, timeout=8) as resp:
                cfg = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return  # fail-open
        self.update_url = cfg.get("update_url", "")
        latest = int(cfg.get("latest_version_code", 0) or 0)
        minimum = int(cfg.get("min_version_code", 0) or 0)
        if PC_VERSION_CODE < minimum:
            self.version_blocked = True
            self.root.after(0, lambda: self._show_update_dialog(blocked=True))
        elif PC_VERSION_CODE < latest:
            self.root.after(0, lambda: self._show_update_dialog(blocked=False))

    def _show_update_dialog(self, blocked):
        t = STR[self.settings["ui_lang"]]
        win = tk.Toplevel(self.root)
        win.title(t["update_title"])
        win.configure(bg=BG)
        win.geometry("380x180")
        win.transient(self.root)
        msg = t["blocked_body"] if blocked else t["update_body"]
        tk.Label(win, bg=BG, fg=TEXT, wraplength=340, justify="center",
                 font=("Segoe UI", 11), text=msg).pack(padx=16, pady=(20, 12))
        row = tk.Frame(win, bg=BG)
        row.pack()
        tk.Button(row, bg=ACCENT, fg="#06222B", bd=0, padx=14, pady=6,
                  text=t["update_now"],
                  command=lambda: (webbrowser.open(self.update_url or
                                                   "https://github.com/stupidiots/AI-Live-Translator/releases/latest"),
                                   win.destroy())).pack(side="left")
        if not blocked:
            tk.Button(row, bg=PANEL, fg=TEXT, bd=0, padx=14, pady=6,
                      text=t["later"], command=win.destroy).pack(
                          side="left", padx=(10, 0))
        else:
            win.protocol("WM_DELETE_WINDOW", lambda: None)

    # ---------------- overlay ----------------
    def _toggle_overlay(self):
        if self.overlay is not None:
            self.overlay.destroy()
            self.overlay = None
            return
        win = tk.Toplevel(self.root)
        win.overrideredirect(True)
        win.attributes("-topmost", True)
        try:
            win.attributes("-alpha", 0.88)
        except Exception:
            pass
        win.configure(bg="#05080F")
        self.overlay_tran = tk.Label(win, bg="#05080F", fg="#FFFFFF",
                                     font=("Segoe UI", 16, "bold"),
                                     wraplength=760, justify="center")
        self.overlay_tran.pack(padx=18, pady=(10, 2))
        self.overlay_orig = tk.Label(win, bg="#05080F", fg=MUTED,
                                     font=("Segoe UI", 10), wraplength=760,
                                     justify="center")
        self.overlay_orig.pack(padx=18, pady=(0, 10))
        sw = win.winfo_screenwidth()
        sh = win.winfo_screenheight()
        win.geometry(f"800x110+{max(0, (sw - 800) // 2)}+{max(0, sh - 190)}")
        drag = {"x": 0, "y": 0}

        def down(e):
            drag["x"], drag["y"] = e.x, e.y

        def move(e):
            win.geometry(f"+{e.x_root - drag['x']}+{e.y_root - drag['y']}")

        for w in (win, self.overlay_tran, self.overlay_orig):
            w.bind("<Button-1>", down)
            w.bind("<B1-Motion>", move)
        self.overlay = win

    # ---------------- events ----------------
    def _append(self, widget, text):
        widget.insert("end", text)
        widget.see("end")

    def _pump(self):
        t = STR[self.settings["ui_lang"]]
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "ready":
                    self.is_ready = True
                    self.reconnects = 0
                    self.status_lbl.config(text=t["live"], fg=ACCENT)
                elif kind == "original":
                    self.orig_line += payload
                    self._append(self.orig_text, payload)
                    if self.overlay_orig is not None:
                        self.overlay_orig.config(text=self.orig_line[-160:])
                elif kind == "translated":
                    self.tran_line += payload
                    self._append(self.tran_text, payload)
                    if self.overlay_tran is not None:
                        self.overlay_tran.config(text=self.tran_line[-200:])
                elif kind == "audio":
                    self.engine.play_dub(payload)
                elif kind == "interrupted":
                    self.engine.flush_dub()
                elif kind == "turn":
                    if self.orig_line and not self.orig_line.endswith("\n"):
                        self._append(self.orig_text, "\n")
                    if self.tran_line and not self.tran_line.endswith("\n"):
                        self._append(self.tran_text, "\n")
                    self.orig_line = ""
                    self.tran_line = ""
                elif kind == "goaway":
                    if self.session_active:
                        self.client.reconnect()
                elif kind == "closed":
                    if self.session_active:
                        self._maybe_reconnect(t)
                elif kind == "auth_error":
                    if self.session_active:
                        self._stop_session()
                    self.status_lbl.config(text=t["err_prefix"] + str(payload),
                                           fg="#F87171")
                elif kind == "error":
                    if self.session_active and self.is_ready:
                        self._maybe_reconnect(t)
                    elif self.session_active:
                        self.status_lbl.config(text=t["err_prefix"] +
                                               str(payload), fg="#F87171")
        except queue.Empty:
            pass
        self.root.after(60, self._pump)

    def _maybe_reconnect(self, t):
        if self.reconnects >= 5:
            self._stop_session()
            self.status_lbl.config(text=t["err_prefix"] + "connection lost",
                                   fg="#F87171")
            return
        self.reconnects += 1
        self.is_ready = False
        self.status_lbl.config(text=t["reconnecting"], fg=ACCENT)
        self.root.after(2500, lambda: self.session_active and
                        self.client.reconnect())

    def _on_close(self):
        try:
            self.client.stop()
            self.engine.stop()
        except Exception:
            pass
        self.root.destroy()


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
