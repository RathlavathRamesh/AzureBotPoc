// ─── Aegis.ai Media Bot — Streaming Entry Point ─────────────
// Endpoints (per Teams application-hosted media bot blueprint):
//   POST /api/messages                       Bot Framework messaging
//   POST /api/calling                        Incoming call webhook (bearer token)
//   POST /api/calls/{callbackToken}/callback Per-call Graph notifications
//   POST /api/acs-events                     ACS CloudEvents callback
//   WS   /ws                                 ACS bidirectional media streaming
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
    await handler.HandleCallEventAsync(BinaryData.FromBytes(ms.ToArray()), callbackToken);
    return Results.Ok();
});

// Backwards-compatible /api/calls (legacy callback — same handler) ───
app.MapPost("/api/calls", async (HttpRequest request, CallHandler handler) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);
    await handler.HandleCallEventAsync(BinaryData.FromBytes(ms.ToArray()));
    return Results.Ok();
});

// ─── ACS CloudEvents ────────────────────────────────────────

app.MapPost("/api/acs-events", async (HttpRequest request, CallHandler handler) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);
    await handler.HandleAcsEventAsync(BinaryData.FromBytes(ms.ToArray()));
    return Results.Ok();
});

// ─── ACS media streaming WebSocket ──────────────────────────
// ACS connects here. Duplex: we receive inbound frames, we write TTS
// OutStreamingData back to the same socket.

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

app.Logger.LogInformation("Aegis Media Bot (streaming) on http://localhost:5000");
app.Run("http://localhost:5000");

// ─── Request models ─────────────────────────────────────────

record JoinRequest(string MeetingUrl, string? MeetingId = null, string? Passcode = null);
record LeaveRequest(string CallId);
