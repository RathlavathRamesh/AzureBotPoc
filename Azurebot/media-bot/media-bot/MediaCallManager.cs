// ─── MediaCallManager ────────────────────────────────────────
// Thin wrapper around Microsoft.Graph.Communications.Calls.Media so we can
// join Teams meetings with appHostedMediaConfig and get raw PCM audio.
//
// Flow:
//   1. JoinMeetingAsync → build a MediaSession → hand it to
//      JoinMeetingParameters (app-hosted constructor) →
//      Calls.AddAsync starts the call.
//   2. On call "Established", attach a BotMediaStream to the MediaSession's
//      AudioSocket so utterances get posted to /api/audio-in.
//   3. Outbound speech continues via CallHandler.PlayPromptAsync (unchanged)
//      — appHostedMediaConfig supports both raw frames AND playPrompt.
//
// Windows-only: Microsoft.Skype.Bots.Media has native Win32 dependencies.
// ─────────────────────────────────────────────────────────────

using System.Collections.Concurrent;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Client;
using Microsoft.Graph.Communications.Client.Authentication;
using Microsoft.Graph.Communications.Common.Telemetry;
using Microsoft.Graph.Communications.Resources;
using Microsoft.Graph.Models;
using Microsoft.Skype.Bots.Media;

namespace MediaBot;

public class MediaCallManager : IDisposable
{
    private readonly IConfiguration _config;
    private readonly ILogger<MediaCallManager> _logger;
    private readonly IHttpClientFactory _httpFactory;
    private readonly IGraphLogger _graphLogger = new GraphLogger(nameof(MediaCallManager));

    private ICommunicationsClient? _client;
    private bool _mediaPlatformInitialized;

    private readonly ConcurrentDictionary<string, MediaCall> _calls = new();

    public MediaCallManager(
        IConfiguration config,
        ILogger<MediaCallManager> logger,
        IHttpClientFactory httpFactory)
    {
        _config = config;
        _logger = logger;
        _httpFactory = httpFactory;
    }

    public void Initialize()
    {
        var appId = _config["Bot:AppId"]!;
        var appSecret = _config["Bot:AppSecret"]!;
        var callback = _config["CallbackUrl"]!;
        var serviceHost = new Uri(callback).Host;

        // 1. Skype media platform — must be initialized exactly once per process.
        try
        {
            MediaPlatform.Initialize(new MediaPlatformSettings
            {
                ApplicationId = appId,
                MediaPlatformInstanceSettings = new MediaPlatformInstanceSettings
                {
                    CertificateThumbprint = _config["Bot:CertThumbprint"] ?? "",
                    InstanceInternalPort = 8445,
                    InstancePublicIPAddress = System.Net.IPAddress.Any,
                    InstancePublicPort = 443,
                    ServiceFqdn = serviceHost,
                },
            });
            _mediaPlatformInitialized = true;
            _logger.LogInformation("[media] platform initialized (fqdn={Host})", serviceHost);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[media] MediaPlatform.Initialize failed — "
                + "app-hosted media will NOT work. Check Bot:CertThumbprint.");
        }

        // 2. Communications client.
        var builder = new CommunicationsClientBuilder("AegisBot", appId, _graphLogger);
        builder.SetAuthenticationProvider(new AuthenticationProvider(appId, appSecret));
        builder.SetNotificationUrl(new Uri($"{callback}/api/calls"));
        builder.SetServiceBaseUrl(new Uri("https://graph.microsoft.com/beta"));

        _client = builder.Build();
        _client.Calls().OnIncoming += OnIncomingCall;
        _client.Calls().OnUpdated += OnCallsUpdated;

        _logger.LogInformation("[media] ICommunicationsClient ready");
    }

    /// <summary>
    /// Join a Teams meeting with app-hosted media. Supply EITHER a full
    /// /l/meetup-join URL (extracted internally) OR a meetingId + passcode.
    /// </summary>
    public async Task<string> JoinMeetingAsync(
        string meetingUrl, string? meetingId = null, string? passcode = null)
    {
        if (_client == null)
            throw new InvalidOperationException("MediaCallManager not initialized");
        if (!_mediaPlatformInitialized)
            throw new InvalidOperationException("Media platform not initialized");

        var tenantId = _config["Bot:TenantId"]!;

        var chatInfo = new ChatInfo { OdataType = "#microsoft.graph.chatInfo" };
        var meetingInfo = new JoinMeetingIdMeetingInfo
        {
            OdataType = "#microsoft.graph.joinMeetingIdMeetingInfo",
            JoinMeetingId = meetingId ?? ExtractMeetingId(meetingUrl),
            Passcode = passcode ?? ExtractPasscode(meetingUrl),
        };

        var mediaSession = BuildMediaSession();

        var join = new JoinMeetingParameters(chatInfo, meetingInfo, mediaSession);
        var call = await _client.Calls().AddAsync(join, new Guid(tenantId));

        _logger.LogInformation("[media] join requested → callId={Id}", call.Id);
        _calls[call.Id] = new MediaCall
        {
            CallId = call.Id, Call = call, MediaSession = mediaSession,
        };
        return call.Id;
    }

    private MediaSession BuildMediaSession()
    {
        var audioSettings = new AudioSocketSettings
        {
            StreamDirections = StreamDirection.Sendrecv,
            SupportedAudioFormat = AudioFormat.Pcm16K,
            CallId = Guid.NewGuid().ToString(),
        };

        return new MediaSession(
            _graphLogger,
            Guid.NewGuid(),
            audioSettings,
            Enumerable.Empty<VideoSocketSettings>(),
            null,
            null);
    }

    private void OnIncomingCall(object? sender, CollectionEventArgs<ICall> args)
    {
        foreach (var call in args.AddedResources)
        {
            _logger.LogInformation("[media] incoming call: {Id}", call.Id);
            var mediaSession = BuildMediaSession();
            call.AnswerAsync(mediaSession).Wait();
            _calls[call.Id] = new MediaCall
            {
                CallId = call.Id, Call = call, MediaSession = mediaSession,
            };
        }
    }

    private void OnCallsUpdated(object? sender, CollectionEventArgs<ICall> args)
    {
        foreach (var call in args.AddedResources)
            call.OnUpdated += OnCallUpdated;
    }

    private void OnCallUpdated(ICall call, ResourceEventArgs<Call> args)
    {
        var state = call.Resource?.State;
        _logger.LogInformation("[media] call {Id} state: {State}", call.Id, state);

        if (state == CallState.Established
            && _calls.TryGetValue(call.Id, out var mc)
            && mc.MediaStream == null
            && mc.MediaSession != null)
        {
            var audioSocket = mc.MediaSession.AudioSocket;
            var botBase = "http://localhost:5000"; // loopback to our own /api/audio-in
            mc.MediaStream = new BotMediaStream(
                audioSocket, call.Id, botBase, _httpFactory, _logger);
            _logger.LogInformation("[media] ✓ audio capture attached for {Id}", call.Id);
        }

        if (state == CallState.Terminated)
        {
            if (_calls.TryRemove(call.Id, out var ended))
            {
                ended.MediaStream?.Dispose();
                ended.MediaSession?.Dispose();
            }
        }
    }

    public void SetBotSpeaking(string callId, bool speaking)
    {
        if (_calls.TryGetValue(callId, out var mc))
            mc.MediaStream?.SetBotSpeaking(speaking);
    }

    public IEnumerable<string> ActiveCallIds => _calls.Keys;

    public void Dispose()
    {
        foreach (var mc in _calls.Values)
        {
            mc.MediaStream?.Dispose();
            mc.MediaSession?.Dispose();
        }
        _calls.Clear();
        GC.SuppressFinalize(this);
    }

    // ─── URL parsers (copied from CallHandler) ──────────────

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

    // ─── Auth provider ──────────────────────────────────────

    private class AuthenticationProvider : IRequestAuthenticationProvider
    {
        private readonly string _appId;
        private readonly string _appSecret;

        public AuthenticationProvider(string appId, string appSecret)
        {
            _appId = appId;
            _appSecret = appSecret;
        }

        public async Task AuthenticateOutboundRequestAsync(HttpRequestMessage request, string tenant)
        {
            using var http = new HttpClient();
            var resp = await http.PostAsync(
                $"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
                new FormUrlEncodedContent(new Dictionary<string, string>
                {
                    ["client_id"] = _appId,
                    ["client_secret"] = _appSecret,
                    ["scope"] = "https://graph.microsoft.com/.default",
                    ["grant_type"] = "client_credentials",
                }));
            var body = await resp.Content.ReadAsStringAsync();
            var doc = System.Text.Json.JsonDocument.Parse(body);
            var token = doc.RootElement.GetProperty("access_token").GetString()!;
            request.Headers.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
        }

        public Task<RequestValidationResult> ValidateInboundRequestAsync(HttpRequestMessage request)
            => Task.FromResult(new RequestValidationResult { IsValid = true });
    }

    private class MediaCall
    {
        public string CallId { get; set; } = "";
        public ICall? Call { get; set; }
        public MediaSession? MediaSession { get; set; }
        public BotMediaStream? MediaStream { get; set; }
    }
}
