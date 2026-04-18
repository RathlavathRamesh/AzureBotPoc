// ─── Aegis.ai Media Bot — Streaming Entry Point ─────────────
// Endpoints (per Teams application-hosted media bot blueprint):
//   POST /api/messages                       Bot Framework messaging
//   POST /api/calling                        Incoming call webhook (bearer token)
//   POST /api/calls/{callbackToken}/callback Per-call Graph notifications
//   WS   /ws                                 Bidirectional PCM media streaming
//   POST /api/join                           Python → bot: join a meeting
//   POST /api/leave                          Python → bot: leave a meeting
//   GET  /api/status, /health                diagnostics
// ─────────────────────────────────────────────────────────────

using MediaBot;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddHttpClient();
builder.Services.AddSingleton<CallHandler>();

var app = builder.Build();

var callHandler = app.Services.GetRequiredService<CallHandler>();
callHandler.Initialize();

app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(30) });

// Audio files directory — C# writes TTS MP3s here, Graph fetches via ngrok.
var audioDir = Path.Combine(app.Environment.ContentRootPath, "audio_files");
Directory.CreateDirectory(audioDir);

// ─── Health ─────────────────────────────────────────────────

app.MapGet("/health", () => Results.Ok(new
{
    service = "aegis-media-bot",
    status = "ok",
    runtime = System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription,
}));

// ─── /api/messages (Bot Framework activities) ───────────────

app.MapPost("/api/messages", async (HttpRequest request) =>
{
    using var reader = new StreamReader(request.Body);
    var body = await reader.ReadToEndAsync();
    app.Logger.LogInformation("/api/messages: {Body}", body[..Math.Min(300, body.Length)]);
    // For the POC we acknowledge activities; meaningful chat handling is
    // deferred to a future iteration (not needed for the voice path).
    return Results.Ok(new { type = "message", text = "Aegis voice bot online" });
});

// ─── /api/calling (incoming call invitations) ───────────────

app.MapPost("/api/calling", async (HttpRequest request, CallHandler handler) =>
{
    var ok = await handler.HandleIncomingCallAsync(request);
    return ok ? Results.Ok() : Results.Unauthorized();
});

// ─── /api/calls/{callbackToken}/callback (per-call Graph events) ──

app.MapPost("/api/calls/{callbackToken}/callback", async (
    string callbackToken, HttpRequest request, CallHandler handler) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);
    await handler.HandleCallEventAsync(ms.ToArray(), callbackToken);
    return Results.Ok();
});

// Backwards-compatible /api/calls (legacy callback — same handler) ───
app.MapPost("/api/calls", async (HttpRequest request, CallHandler handler) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);
    await handler.HandleCallEventAsync(ms.ToArray());
    return Results.Ok();
});

// ─── Media streaming WebSocket ──────────────────────────────
// The Teams media transport connects here. Duplex: we receive inbound PCM
// frames and write TTS PCM chunks back on the same socket.

app.Map("/ws", async (HttpContext context, CallHandler handler) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = 400;
        await context.Response.WriteAsync("WebSocket expected");
        return;
    }
    using var ws = await context.WebSockets.AcceptWebSocketAsync();
    await handler.HandleAudioStreamAsync(ws);
});

// ─── Control plane (Python → bot) ───────────────────────────

app.MapPost("/api/join", async (JoinRequest request, CallHandler handler) =>
{
    var result = await handler.JoinMeetingAsync(request.MeetingUrl, request.MeetingId, request.Passcode);
    return result.Success ? Results.Ok(result) : Results.BadRequest(result);
});

app.MapPost("/api/leave", async (LeaveRequest request, CallHandler handler) =>
{
    var success = await handler.LeaveMeetingAsync(request.CallId);
    return Results.Ok(new { success });
});

app.MapGet("/api/status", (CallHandler handler) => Results.Ok(handler.GetStatus()));

// ─── Batch voice plumbing ───────────────────────────────────
// Serve audio files (Graph playPrompt downloads them via ngrok)
app.MapGet("/audio/{filename}", (string filename) =>
{
    var filepath = Path.Combine(audioDir, filename);
    if (!File.Exists(filepath)) return Results.NotFound();
    return Results.File(filepath, "audio/mpeg");
});

// /api/audio-in — the entry point from whichever component captures Teams
// audio (real media SDK, test tool, simulator). Posts the audio to Python
// /process-audio and plays the response in the meeting.
app.MapPost("/api/audio-in", async (HttpRequest request, CallHandler handler) =>
{
    using var reader = new StreamReader(request.Body);
    var bodyStr = await reader.ReadToEndAsync();
    using var doc = System.Text.Json.JsonDocument.Parse(bodyStr);

    var callId = doc.RootElement.GetProperty("callId").GetString() ?? "";
    var audioB64 = doc.RootElement.GetProperty("audioBase64").GetString() ?? "";

    if (string.IsNullOrEmpty(callId) || string.IsNullOrEmpty(audioB64))
        return Results.BadRequest(new { error = "callId and audioBase64 are required" });

    var userAudio = Convert.FromBase64String(audioB64);
    app.Logger.LogInformation("/api/audio-in: call={CallId} bytes={N}", callId, userAudio.Length);

    var result = await handler.ProcessAudioRoundTripAsync(callId, userAudio, audioDir);
    return Results.Ok(result);
});

// /api/speak — accept already-synthesized audio bytes (MP3) and play them.
// Useful if Python wants to push audio independently of the round-trip.
app.MapPost("/api/speak", async (HttpRequest request, CallHandler handler) =>
{
    using var reader = new StreamReader(request.Body);
    var bodyStr = await reader.ReadToEndAsync();
    using var doc = System.Text.Json.JsonDocument.Parse(bodyStr);

    var callId = doc.RootElement.GetProperty("callId").GetString() ?? "";
    var audioB64 = doc.RootElement.GetProperty("audioBase64").GetString() ?? "";

    if (string.IsNullOrEmpty(callId) || string.IsNullOrEmpty(audioB64))
        return Results.BadRequest(new { error = "callId and audioBase64 required" });

    var audio = Convert.FromBase64String(audioB64);
    var filename = $"{Guid.NewGuid():N}.mp3";
    var filepath = Path.Combine(audioDir, filename);
    await File.WriteAllBytesAsync(filepath, audio);

    var audioUrl = $"{app.Configuration["CallbackUrl"]}/audio/{filename}";
    app.Logger.LogInformation("/api/speak: call={CallId} url={Url}", callId, audioUrl);

    var ok = await handler.PlayPromptAsync(callId, audioUrl);
    return Results.Ok(new { success = ok, audioUrl, size = audio.Length });
});

app.Logger.LogInformation("Aegis Media Bot (streaming) on http://localhost:5000");
app.Run("http://localhost:5000");

// ─── Request models ─────────────────────────────────────────

record JoinRequest(string MeetingUrl, string? MeetingId = null, string? Passcode = null);
record LeaveRequest(string CallId);
