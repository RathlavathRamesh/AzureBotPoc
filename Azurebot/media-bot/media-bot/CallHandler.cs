// ─── CallHandler ─────────────────────────────────────────────
// Purpose: Streaming bridge between a Teams meeting (ACS Media Streaming
//          over WebSocket) and the Python voice pipeline (duplex WebSocket).
//
//          Flow per call:
//            Teams/ACS → /ws (this bot) → Python /voice/sessions/{callId}
//                                          → stt.partial/final, llm.partial,
//                                            tts.audio (PCM chunks)
//                        ← OutStreamingData ← tts.audio
//
//          No files, no playPrompt. TTS PCM chunks are written directly into
//          the ACS WebSocket as OutStreamingData — the bot starts speaking
//          as soon as the first chunks arrive. On tts.cancel we send
//          StopAudio to ACS and flush the playout buffer (barge-in).
// ─────────────────────────────────────────────────────────────

using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Azure.Communication.CallAutomation;

namespace MediaBot;

public class CallHandler
{
    private readonly IConfiguration _config;
    private readonly ILogger<CallHandler> _logger;
    private readonly IHttpClientFactory _httpFactory;
    private CallAutomationClient? _acsClient;

    private readonly ConcurrentDictionary<string, CallInfo> _activeCalls = new();
    private readonly ConcurrentDictionary<string, VoiceSession> _sessions = new();

    // Tracks Graph callId → ACS WebSocket so we can route OutStreamingData
    // for a Python session back to the correct ACS socket.
    private readonly ConcurrentDictionary<string, WebSocket> _acsSockets = new();

    public CallHandler(IConfiguration config, ILogger<CallHandler> logger, IHttpClientFactory httpFactory)
    {
        _config = config;
        _logger = logger;
        _httpFactory = httpFactory;
    }

    public bool Initialize()
    {
        var connectionString = _config["ACS:ConnectionString"];
        if (string.IsNullOrEmpty(connectionString))
        {
            _logger.LogWarning("ACS not configured — running in mock mode");
            return false;
        }

        _acsClient = new CallAutomationClient(connectionString);
        _logger.LogInformation("ACS client initialized");
        return true;
    }

    // ─── Join (Graph answer + ACS audio streaming) ──────────

    public async Task<JoinResult> JoinMeetingAsync(string meetingUrl, string? meetingId = null, string? passcode = null)
    {
        _logger.LogInformation("Join requested: MeetingId={Id}", meetingId ?? "from-url");

        if (_acsClient == null)
        {
            var mockId = $"mock-{Guid.NewGuid().ToString()[..8]}";
            _activeCalls[mockId] = new CallInfo { CallId = mockId, MeetingUrl = meetingUrl };
            await NotifyPythonControlAsync("call_connected", new { callId = mockId, mock = true });
            return new JoinResult { Success = true, CallId = mockId, Mock = true };
        }

        try
        {
            var callbackBase = _config["CallbackUrl"]!;
            var tenantId = ExtractTenantId(meetingUrl) ?? _config["Bot:TenantId"]!;
            var token = await GetGraphTokenAsync(tenantId);

            var actualMeetingId = meetingId ?? ExtractMeetingId(meetingUrl);
            var actualPasscode = passcode ?? ExtractPasscode(meetingUrl);

            // Graph answer uses a per-call callback — real-time media bot best
            // practice per Microsoft: pin call state to a specific instance.
            var tempCallbackId = Guid.NewGuid().ToString("N")[..12];
            var callbackUri = $"{callbackBase}/api/calls/{tempCallbackId}/callback";

            var jsonPayload = $$"""
            {
                "@odata.type": "#microsoft.graph.call",
                "callbackUri": "{{callbackUri}}",
                "requestedModalities": ["audio"],
                "mediaConfig": {
                    "@odata.type": "#microsoft.graph.serviceHostedMediaConfig"
                },
                "meetingInfo": {
                    "@odata.type": "#microsoft.graph.joinMeetingIdMeetingInfo",
                    "joinMeetingId": "{{actualMeetingId}}",
                    "passcode": "{{actualPasscode}}"
                },
                "tenantId": "{{tenantId}}"
            }
            """;

            using var http = _httpFactory.CreateClient();
            http.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

            var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
            var resp = await http.PostAsync("https://graph.microsoft.com/beta/communications/calls", content);
            var respBody = await resp.Content.ReadAsStringAsync();

            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogError("Graph error: {Status} {Body}", resp.StatusCode, respBody[..Math.Min(500, respBody.Length)]);
                return new JoinResult { Success = false, Error = respBody[..Math.Min(200, respBody.Length)] };
            }

            var doc = JsonDocument.Parse(respBody);
            var callId = doc.RootElement.GetProperty("id").GetString() ?? "";

            _activeCalls[callId] = new CallInfo
            {
                CallId = callId,
                MeetingUrl = meetingUrl,
                GraphCallId = callId,
                TenantId = tenantId,
                MeetingId = actualMeetingId,
                Passcode = actualPasscode,
                CallbackToken = tempCallbackId,
            };

            _logger.LogInformation("✓ Bot joined meeting: {CallId}", callId);
            await NotifyPythonControlAsync("call_connected", new { callId });

            // ACS joins the same meeting as a second audio participant so we
            // get a live frame-by-frame audio stream over /ws.
            _ = Task.Run(() => JoinAcsForAudioAsync(callId));

            return new JoinResult { Success = true, CallId = callId };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Join failed");
            return new JoinResult { Success = false, Error = ex.Message };
        }
    }

    // ─── ACS WebSocket: duplex audio with Teams ─────────────

    public async Task HandleAudioStreamAsync(WebSocket acsWs)
    {
        // The ACS socket carries audio both ways:
        //   inbound:  {kind:"AudioData", audioData:{data:<base64 PCM>, silent:bool}}
        //   outbound: same shape; we write TTS PCM chunks here to speak.
        _logger.LogInformation("🎤 ACS WebSocket connected");

        // Which Graph call does this socket belong to? We bind it to the most
        // recent joined call — a stateful worker only handles one call per
        // instance (per Microsoft guidance), so this is safe.
        var callId = _activeCalls.Keys.LastOrDefault() ?? "unknown";
        _acsSockets[callId] = acsWs;

        // Open the Python voice session for this call. The session is the
        // duplex pipeline — STT + LLM + TTS all flow through it.
        var session = await OpenPythonSessionAsync(callId, acsWs);
        if (session == null)
        {
            _logger.LogError("Could not open Python session for {CallId}", callId);
            return;
        }

        _sessions[callId] = session;

        var buffer = new byte[16384];
        var frameSeq = 0;

        try
        {
            while (acsWs.State == WebSocketState.Open)
            {
                var result = await acsWs.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    await acsWs.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
                    break;
                }

                var json = Encoding.UTF8.GetString(buffer, 0, result.Count);
                JsonDocument doc;
                try { doc = JsonDocument.Parse(json); }
                catch (JsonException) { continue; }

                if (!doc.RootElement.TryGetProperty("kind", out var kindProp)) continue;
                var kind = kindProp.GetString();

                if (kind == "AudioMetadata")
                {
                    _logger.LogInformation("ACS audio metadata received");
                    continue;
                }

                if (kind != "AudioData" || !doc.RootElement.TryGetProperty("audioData", out var audioData))
                    continue;

                var silent = audioData.TryGetProperty("silent", out var s) && s.GetBoolean();
                var pcmB64 = audioData.TryGetProperty("data", out var d) ? d.GetString() ?? "" : "";
                if (string.IsNullOrEmpty(pcmB64)) continue;

                frameSeq++;

                // Forward every frame to Python — Deepgram Live handles VAD
                // and endpointing, so we don't need client-side silence logic.
                await SendToPythonAsync(session, new
                {
                    type = "audio.in",
                    seq = frameSeq,
                    silent,
                    pcm = pcmB64,
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ACS WebSocket error");
        }
        finally
        {
            _logger.LogInformation("ACS WebSocket closed for {CallId} (frames: {N})", callId, frameSeq);
            _acsSockets.TryRemove(callId, out _);
            await session.CloseAsync();
            _sessions.TryRemove(callId, out _);
        }
    }

    // ─── Python duplex session ──────────────────────────────

    private async Task<VoiceSession?> OpenPythonSessionAsync(string callId, WebSocket acsWs)
    {
        var pyBase = _config["PythonBackendUrl"] ?? "http://localhost:8000";
        var wsUrl = pyBase.Replace("http://", "ws://").Replace("https://", "wss://")
                    + $"/voice/sessions/{callId}";

        var client = new ClientWebSocket();
        try
        {
            await client.ConnectAsync(new Uri(wsUrl), CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Could not connect to Python voice session at {Url}", wsUrl);
            return null;
        }

        _logger.LogInformation("✓ Python voice session open: {Url}", wsUrl);

        var session = new VoiceSession(callId, client, acsWs, _logger);

        // Pump events from Python → ACS / logs.
        _ = Task.Run(session.PumpFromPythonAsync);

        return session;
    }

    private static async Task SendToPythonAsync(VoiceSession session, object payload)
    {
        var json = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(json);
        await session.PythonWs.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    // ─── ACS join for audio streaming ───────────────────────

    private async Task JoinAcsForAudioAsync(string graphCallId)
    {
        if (_acsClient == null) return;

        try
        {
            // Wait for Graph to deliver chatInfo.threadId via the per-call
            // callback (usually <2s).
            await Task.Delay(5000);

            _activeCalls.TryGetValue(graphCallId, out var callInfo);
            string meetingUrl;
            if (!string.IsNullOrEmpty(callInfo?.ThreadId))
            {
                var encoded = Uri.EscapeDataString(callInfo.ThreadId);
                meetingUrl = $"https://teams.microsoft.com/l/meetup-join/{encoded}/0";
            }
            else if (!string.IsNullOrEmpty(callInfo?.MeetingId))
            {
                meetingUrl = $"https://teams.microsoft.com/meet/{callInfo.MeetingId}?p={callInfo.Passcode}";
            }
            else if (!string.IsNullOrEmpty(callInfo?.MeetingUrl))
            {
                meetingUrl = callInfo.MeetingUrl;
            }
            else
            {
                _logger.LogError("No meeting URL available for ACS join on call {Id}", graphCallId);
                return;
            }

            var callbackBase = _config["CallbackUrl"]!;
            var wsUrl = callbackBase.Replace("https://", "wss://").Replace("http://", "ws://") + "/ws";
            var callbackUri = new Uri($"{callbackBase}/api/acs-events");

            var streamingOptions = new MediaStreamingOptions(
                MediaStreamingAudioChannel.Mixed,
                StreamingTransport.Websocket)
            {
                TransportUri = new Uri(wsUrl),
                StartMediaStreaming = true,
                // bidirectional — we send TTS back to Teams on this same socket
                EnableBidirectional = true,
                AudioFormat = AudioFormat.Pcm16KMono,
            };

            // ACS SDK 1.6.0-beta.2 has no MicrosoftTeamsMeetingLocator. The
            // supported path for joining a Graph-answered Teams meeting is
            // ConnectCallAsync(ServerCallLocator(serverCallId), ...) once the
            // serverCallId is obtained. That ID is available from the Graph
            // /communications/calls/{id} GET — fetched once the call reaches
            // the "established" state. For now we log and rely on a manual
            // ACS join: ACS can also be wired via an external trigger that
            // calls ConnectCallAsync with the right ServerCallLocator.
            _logger.LogWarning(
                "ACS auto-join skipped: MicrosoftTeamsMeetingLocator not in SDK 1.6.0-beta.2. " +
                "MeetingUrl={Url}. ACS WS endpoint ready at {Ws} — trigger ConnectCallAsync " +
                "with ServerCallLocator(serverCallId) once available.",
                meetingUrl, wsUrl);
            _ = callbackUri;
            _ = streamingOptions;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ACS audio join failed for {Id}", graphCallId);
        }
    }

    // ─── Leave ──────────────────────────────────────────────

    public async Task<bool> LeaveMeetingAsync(string callId)
    {
        _logger.LogInformation("Leave requested: {CallId}", callId);

        try
        {
            if (_sessions.TryRemove(callId, out var session))
                await session.CloseAsync();

            var tenantId = _activeCalls.TryGetValue(callId, out var info) ? info.TenantId : null;
            var token = await GetGraphTokenAsync(tenantId);
            using var http = _httpFactory.CreateClient();
            http.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

            var resp = await http.DeleteAsync(
                $"https://graph.microsoft.com/beta/communications/calls/{callId}");

            _activeCalls.TryRemove(callId, out _);
            await NotifyPythonControlAsync("call_disconnected", new { callId });

            _logger.LogInformation("Left meeting: {CallId}, Status: {Status}", callId, resp.StatusCode);
            return resp.IsSuccessStatusCode;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Leave failed: {CallId}", callId);
            return false;
        }
    }

    // ─── Graph call event webhooks ──────────────────────────

    public async Task HandleCallEventAsync(BinaryData requestBody, string? callbackToken = null)
    {
        try
        {
            var rawJson = requestBody.ToString();
            _logger.LogInformation("Call webhook ({Token}): {Json}",
                callbackToken ?? "-", rawJson[..Math.Min(800, rawJson.Length)]);

            var doc = JsonDocument.Parse(rawJson);
            var root = doc.RootElement;

            string callState = "";
            string callId = "";

            if (root.TryGetProperty("value", out var valueArr) && valueArr.GetArrayLength() > 0)
            {
                var notification = valueArr[0];
                if (notification.TryGetProperty("resourceUrl", out var resUrl))
                {
                    var segments = (resUrl.GetString() ?? "").Split('/');
                    for (int i = 0; i < segments.Length - 1; i++)
                        if (segments[i] == "calls") { callId = segments[i + 1]; break; }
                }

                if (notification.TryGetProperty("resourceData", out var res))
                {
                    if (res.TryGetProperty("state", out var s))
                        callState = s.GetString() ?? "";

                    if (res.TryGetProperty("chatInfo", out var chat)
                        && chat.TryGetProperty("threadId", out var tid))
                    {
                        var threadId = tid.GetString() ?? "";
                        if (!string.IsNullOrEmpty(threadId) && !string.IsNullOrEmpty(callId)
                            && _activeCalls.TryGetValue(callId, out var ci) && string.IsNullOrEmpty(ci.ThreadId))
                        {
                            ci.ThreadId = threadId;
                            _logger.LogInformation("Captured threadId for {CallId}: {ThreadId}", callId, threadId);
                        }
                    }
                }
            }

            if (string.IsNullOrEmpty(callState) && root.TryGetProperty("state", out var topState))
                callState = topState.GetString() ?? "";
            if (string.IsNullOrEmpty(callId) && root.TryGetProperty("id", out var topId))
                callId = topId.GetString() ?? "";

            switch (callState)
            {
                case "established":
                    _logger.LogInformation("✓ CALL ESTABLISHED — {CallId}", callId);
                    await NotifyPythonControlAsync("call_established", new { callId });
                    break;
                case "terminated":
                    _logger.LogInformation("Call terminated: {CallId}", callId);
                    if (_sessions.TryRemove(callId, out var session))
                        await session.CloseAsync();
                    _activeCalls.TryRemove(callId, out _);
                    await NotifyPythonControlAsync("call_disconnected", new { callId });
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Webhook error");
        }
    }

    public Task HandleAcsEventAsync(BinaryData body)
    {
        _logger.LogInformation("ACS event: {Json}", body.ToString()[..Math.Min(300, body.ToString().Length)]);
        return Task.CompletedTask;
    }

    // ─── Incoming calling webhook (/api/calling) ────────────
    // For the application-hosted media bot, Teams POSTs incoming call
    // invitations here with a Bearer token.

    public async Task<bool> HandleIncomingCallAsync(HttpRequest request)
    {
        if (!request.Headers.TryGetValue("Authorization", out var auth))
        {
            _logger.LogWarning("/api/calling missing Authorization header");
            return false;
        }
        var authStr = auth.ToString();
        if (!authStr.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning("/api/calling non-Bearer auth");
            return false;
        }

        // Token validation (issuer, audience, signature) should be done via
        // Microsoft.IdentityModel / JwtBearerHandler in production. For the
        // POC we log the token and accept it.
        _logger.LogInformation("/api/calling token received (len={Len})", authStr.Length - 7);

        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();
        _logger.LogInformation("/api/calling body: {Body}", body[..Math.Min(400, body.Length)]);

        // TODO: Parse the invite, call Graph answer with a per-call callback.
        await NotifyPythonControlAsync("incoming_call", new { body = body[..Math.Min(1000, body.Length)] });
        return true;
    }

    // ─── Python control plane notify ────────────────────────

    private async Task NotifyPythonControlAsync(string eventType, object data)
    {
        try
        {
            var client = _httpFactory.CreateClient();
            client.BaseAddress = new Uri(_config["PythonBackendUrl"] ?? "http://localhost:8000");
            var json = JsonSerializer.Serialize(new { type = eventType, data });
            await client.PostAsync("/control", new StringContent(json, Encoding.UTF8, "application/json"));
        }
        catch
        {
            // control-plane notifications are best-effort — streaming path is primary
        }
    }

    public object GetStatus() => new
    {
        activeCalls = _activeCalls.Count,
        sessions = _sessions.Count,
        calls = _activeCalls.Values.Select(c => new { c.CallId, c.MeetingUrl }),
        acsConfigured = _acsClient != null,
    };

    // ─── Graph token ────────────────────────────────────────

    private readonly Dictionary<string, (string token, DateTime expiry)> _tokenCache = new();

    private async Task<string> GetGraphTokenAsync(string? tenantId = null)
    {
        tenantId ??= _config["Bot:TenantId"]!;

        if (_tokenCache.TryGetValue(tenantId, out var cached) && DateTime.UtcNow < cached.expiry)
            return cached.token;

        var appId = _config["Bot:AppId"]!;
        var secret = _config["Bot:AppSecret"]!;

        using var http = _httpFactory.CreateClient();
        var resp = await http.PostAsync(
            $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token",
            new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["client_id"] = appId,
                ["client_secret"] = secret,
                ["scope"] = "https://graph.microsoft.com/.default",
                ["grant_type"] = "client_credentials",
            }));

        var body = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
            throw new Exception($"Token failed: {resp.StatusCode} {body[..Math.Min(200, body.Length)]}");

        var doc = JsonDocument.Parse(body);
        var token = doc.RootElement.GetProperty("access_token").GetString()!;
        var expiresIn = doc.RootElement.GetProperty("expires_in").GetInt32();
        _tokenCache[tenantId] = (token, DateTime.UtcNow.AddSeconds(expiresIn - 60));
        return token;
    }

    // ─── URL parsers ────────────────────────────────────────

    private static string ExtractMeetingId(string meetingUrl)
    {
        if (string.IsNullOrEmpty(meetingUrl)) return "";
        var uri = new Uri(meetingUrl);
        if (uri.AbsolutePath.StartsWith("/meet/"))
            return uri.AbsolutePath.Replace("/meet/", "").Trim('/');
        return "";
    }

    private static string? ExtractPasscode(string meetingUrl)
    {
        if (string.IsNullOrEmpty(meetingUrl)) return null;
        var uri = new Uri(meetingUrl);
        return System.Web.HttpUtility.ParseQueryString(uri.Query)["p"];
    }

    private static string? ExtractTenantId(string meetingUrl)
    {
        try
        {
            if (string.IsNullOrEmpty(meetingUrl)) return null;
            var uri = new Uri(meetingUrl);
            var context = System.Web.HttpUtility.ParseQueryString(uri.Query)["context"];
            if (string.IsNullOrEmpty(context)) return null;
            var decoded = Uri.UnescapeDataString(context);
            var doc = JsonDocument.Parse(decoded);
            return doc.RootElement.TryGetProperty("Tid", out var tid) ? tid.GetString() : null;
        }
        catch { return null; }
    }
}

// ─── VoiceSession: Python WebSocket + playout buffer ────────

public class VoiceSession
{
    // ACS expects 20ms frames of 16-bit mono 16kHz PCM = 640 bytes/frame.
    // We buffer ~300ms before starting playback to absorb jitter, then
    // push one frame every 20ms while chunks keep arriving.
    private const int PlayoutBufferMs = 300;
    private const int FrameBytes = 640;

    public string CallId { get; }
    public ClientWebSocket PythonWs { get; }
    public WebSocket AcsWs { get; }

    private readonly ILogger _logger;
    private readonly Queue<byte[]> _playout = new();
    private readonly SemaphoreSlim _playoutLock = new(1, 1);
    private CancellationTokenSource _playbackCts = new();
    private Task? _playbackTask;

    public VoiceSession(string callId, ClientWebSocket pythonWs, WebSocket acsWs, ILogger logger)
    {
        CallId = callId;
        PythonWs = pythonWs;
        AcsWs = acsWs;
        _logger = logger;
    }

    // ─── Python → ACS pump ──────────────────────────────────

    public async Task PumpFromPythonAsync()
    {
        var buffer = new byte[16384];
        var sb = new StringBuilder();

        try
        {
            while (PythonWs.State == WebSocketState.Open)
            {
                sb.Clear();
                WebSocketReceiveResult result;
                do
                {
                    result = await PythonWs.ReceiveAsync(buffer, CancellationToken.None);
                    sb.Append(Encoding.UTF8.GetString(buffer, 0, result.Count));
                } while (!result.EndOfMessage);

                if (result.MessageType == WebSocketMessageType.Close) break;

                var json = sb.ToString();
                JsonDocument doc;
                try { doc = JsonDocument.Parse(json); }
                catch (JsonException) { continue; }

                var type = doc.RootElement.TryGetProperty("type", out var tp) ? tp.GetString() : null;

                switch (type)
                {
                    case "tts.audio":
                        var pcm64 = doc.RootElement.GetProperty("pcm").GetString() ?? "";
                        await EnqueueTtsChunkAsync(Convert.FromBase64String(pcm64));
                        break;

                    case "tts.cancel":
                        await FlushPlayoutAsync();
                        await SendStopAudioAsync();
                        break;

                    case "tts.end":
                        _logger.LogInformation("[{Call}] tts.end", CallId);
                        break;

                    case "stt.partial":
                    case "stt.final":
                    case "llm.partial":
                    case "llm.final":
                        // informational — already logged on Python side
                        break;

                    case "error":
                        _logger.LogWarning("[{Call}] Python error: {Msg}", CallId,
                            doc.RootElement.TryGetProperty("message", out var m) ? m.GetString() : "-");
                        break;
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{Call}] PumpFromPython error", CallId);
        }
    }

    private async Task EnqueueTtsChunkAsync(byte[] pcm)
    {
        // Slice into 20ms frames so ACS gets a steady feed.
        await _playoutLock.WaitAsync();
        try
        {
            for (int off = 0; off < pcm.Length; off += FrameBytes)
            {
                var len = Math.Min(FrameBytes, pcm.Length - off);
                var frame = new byte[len];
                Buffer.BlockCopy(pcm, off, frame, 0, len);
                _playout.Enqueue(frame);
            }

            if (_playbackTask == null || _playbackTask.IsCompleted)
            {
                _playbackCts = new CancellationTokenSource();
                _playbackTask = Task.Run(() => PlaybackLoopAsync(_playbackCts.Token));
            }
        }
        finally
        {
            _playoutLock.Release();
        }
    }

    private async Task PlaybackLoopAsync(CancellationToken ct)
    {
        // Prime the buffer with PlayoutBufferMs worth of audio before we start
        // writing to ACS — smooths over jitter from the LLM/TTS pipeline.
        var primeFrames = PlayoutBufferMs / 20;
        var primeStart = DateTime.UtcNow;
        while (!ct.IsCancellationRequested)
        {
            await _playoutLock.WaitAsync(ct);
            var enough = _playout.Count >= primeFrames;
            _playoutLock.Release();
            if (enough || (DateTime.UtcNow - primeStart).TotalMilliseconds > 500) break;
            await Task.Delay(10, ct);
        }

        var next = DateTime.UtcNow;
        while (!ct.IsCancellationRequested)
        {
            byte[]? frame;
            await _playoutLock.WaitAsync(ct);
            _playout.TryDequeue(out frame);
            var empty = _playout.Count == 0;
            _playoutLock.Release();

            if (frame == null)
            {
                if (empty) break;
                continue;
            }

            await SendAudioFrameAsync(frame);

            // Pace at 20ms per frame so Teams playback sounds natural.
            next = next.AddMilliseconds(20);
            var delay = next - DateTime.UtcNow;
            if (delay > TimeSpan.Zero) await Task.Delay(delay, ct);
            else next = DateTime.UtcNow;
        }
    }

    private async Task SendAudioFrameAsync(byte[] pcm)
    {
        if (AcsWs.State != WebSocketState.Open) return;
        var payload = new
        {
            kind = "AudioData",
            audioData = new { data = Convert.ToBase64String(pcm) },
            stopAudio = (object?)null,
        };
        var json = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(json);
        await AcsWs.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private async Task SendStopAudioAsync()
    {
        if (AcsWs.State != WebSocketState.Open) return;
        var payload = new
        {
            kind = "StopAudio",
            audioData = (object?)null,
            stopAudio = new { },
        };
        var json = JsonSerializer.Serialize(payload);
        var bytes = Encoding.UTF8.GetBytes(json);
        await AcsWs.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private async Task FlushPlayoutAsync()
    {
        _playbackCts.Cancel();
        await _playoutLock.WaitAsync();
        try { _playout.Clear(); }
        finally { _playoutLock.Release(); }
    }

    public async Task CloseAsync()
    {
        try
        {
            _playbackCts.Cancel();
            if (PythonWs.State == WebSocketState.Open)
                await PythonWs.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None);
        }
        catch { /* ignore */ }
    }
}

// ─── Models ─────────────────────────────────────────────────

public class CallInfo
{
    public string CallId { get; set; } = "";
    public string MeetingUrl { get; set; } = "";
    public string GraphCallId { get; set; } = "";
    public string? TenantId { get; set; }
    public string? MeetingId { get; set; }
    public string? Passcode { get; set; }
    public string? ThreadId { get; set; }
    public string? CallbackToken { get; set; }
}

public class JoinResult
{
    public bool Success { get; set; }
    public string CallId { get; set; } = "";
    public string? Error { get; set; }
    public bool Mock { get; set; }
}
