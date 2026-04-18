// ─── BotMediaStream ──────────────────────────────────────────
// Captures raw audio frames from an app-hosted Teams call, batches them
// into per-utterance WAVs using a simple silence-based VAD, and posts each
// WAV to our existing /api/audio-in endpoint. That endpoint already knows
// how to round-trip: STT → LLM → TTS → playPrompt back to Teams.
//
// The frame format from the Microsoft.Skype.Bots.Media SDK is 16 kHz,
// 16-bit signed, mono PCM — same format our Python /process-audio expects.
// ─────────────────────────────────────────────────────────────

using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Microsoft.Skype.Bots.Media;

namespace MediaBot;

public class BotMediaStream : IDisposable
{
    private readonly IAudioSocket _audioSocket;
    private readonly string _callId;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger _logger;
    private readonly string _botBaseUrl;

    // Audio buffer + silence-based endpointing. Teams audio arrives at
    // 50 frames/sec; we accumulate 16-bit PCM and flush once we see
    // SilenceFramesThreshold consecutive low-energy frames.
    private readonly List<byte[]> _buffer = new();
    private readonly object _bufferLock = new();
    private int _silenceFrames;
    private int _voicedFrames;
    private volatile bool _botIsSpeaking;

    private const int SampleRate = 16000;
    private const int SilenceRmsThreshold = 500;     // tune if too eager/lazy
    private const int SilenceFramesThreshold = 40;   // 40 × 20 ms = 800 ms
    private const int MinVoicedFrames = 10;          // ignore sub-200 ms blips

    public BotMediaStream(
        IAudioSocket audioSocket,
        string callId,
        string botBaseUrl,
        IHttpClientFactory httpFactory,
        ILogger logger)
    {
        _audioSocket = audioSocket;
        _callId = callId;
        _botBaseUrl = botBaseUrl;
        _httpFactory = httpFactory;
        _logger = logger;

        _audioSocket.AudioMediaReceived += OnAudioReceived;
        _audioSocket.AudioSendStatusChanged += OnSendStatusChanged;

        _logger.LogInformation("[media] BotMediaStream attached for call {CallId}", _callId);
    }

    /// <summary>
    /// Suppresses inbound audio capture while the bot is speaking. Call
    /// this when playPrompt starts, clear when it completes.
    /// </summary>
    public void SetBotSpeaking(bool speaking)
    {
        _botIsSpeaking = speaking;
        if (speaking)
        {
            // Drop any in-progress utterance so it doesn't get sent after the bot finishes.
            lock (_bufferLock) { _buffer.Clear(); _silenceFrames = 0; _voicedFrames = 0; }
        }
    }

    private void OnAudioReceived(object? sender, AudioMediaReceivedEventArgs args)
    {
        try
        {
            if (_botIsSpeaking)
            {
                args.Buffer.Dispose();
                return;
            }

            var length = (int)args.Buffer.Length;
            if (length <= 0) { args.Buffer.Dispose(); return; }

            var pcm = new byte[length];
            Marshal.Copy(args.Buffer.Data, pcm, 0, length);

            var isSilent = IsSilent(pcm);

            lock (_bufferLock)
            {
                if (isSilent)
                {
                    _silenceFrames++;
                    if (_voicedFrames >= MinVoicedFrames && _silenceFrames >= SilenceFramesThreshold)
                    {
                        // End of utterance. Hand the buffered PCM off.
                        var chunks = _buffer.ToArray();
                        _buffer.Clear();
                        _silenceFrames = 0;
                        _voicedFrames = 0;

                        _ = Task.Run(() => FlushUtteranceAsync(chunks));
                    }
                }
                else
                {
                    _voicedFrames++;
                    _silenceFrames = 0;
                    _buffer.Add(pcm);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[media] OnAudioReceived error");
        }
        finally
        {
            args.Buffer.Dispose();
        }
    }

    private void OnSendStatusChanged(object? sender, AudioSendStatusChangedEventArgs args)
    {
        _logger.LogInformation("[media] audio send status: {Status}", args.MediaSendStatus);
    }

    private static bool IsSilent(byte[] pcm)
    {
        // RMS over 16-bit samples. Low-CPU VAD — good enough to endpoint utterances.
        long sumSq = 0;
        int samples = pcm.Length / 2;
        for (int i = 0; i < pcm.Length; i += 2)
        {
            short s = (short)(pcm[i] | (pcm[i + 1] << 8));
            sumSq += s * s;
        }
        double rms = Math.Sqrt((double)sumSq / Math.Max(1, samples));
        return rms < SilenceRmsThreshold;
    }

    private async Task FlushUtteranceAsync(byte[][] chunks)
    {
        try
        {
            var wav = BuildWav(chunks);
            var bytes = chunks.Sum(c => c.Length);
            var ms = bytes / 32; // 16 kHz * 2 bytes/sample ≈ 32 bytes/ms
            _logger.LogInformation("[media] utterance complete ({Ms} ms, {N} bytes) → /api/audio-in",
                ms, bytes);

            var payload = JsonSerializer.Serialize(new
            {
                callId = _callId,
                audioBase64 = Convert.ToBase64String(wav),
            });

            var http = _httpFactory.CreateClient();
            http.Timeout = TimeSpan.FromSeconds(90);

            var resp = await http.PostAsync(
                $"{_botBaseUrl}/api/audio-in",
                new StringContent(payload, Encoding.UTF8, "application/json"));

            var body = await resp.Content.ReadAsStringAsync();
            _logger.LogInformation("[media] /api/audio-in replied {Status}: {Body}",
                resp.StatusCode, body[..Math.Min(200, body.Length)]);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[media] utterance flush failed");
        }
    }

    private static byte[] BuildWav(byte[][] chunks)
    {
        var pcm = chunks.SelectMany(c => c).ToArray();
        const short channels = 1;
        const short bitsPerSample = 16;
        int byteRate = SampleRate * channels * bitsPerSample / 8;
        short blockAlign = (short)(channels * bitsPerSample / 8);

        using var ms = new MemoryStream();
        using var w = new BinaryWriter(ms);
        w.Write(Encoding.ASCII.GetBytes("RIFF"));
        w.Write(36 + pcm.Length);
        w.Write(Encoding.ASCII.GetBytes("WAVE"));
        w.Write(Encoding.ASCII.GetBytes("fmt "));
        w.Write(16);               // fmt chunk size
        w.Write((short)1);         // PCM
        w.Write(channels);
        w.Write(SampleRate);
        w.Write(byteRate);
        w.Write(blockAlign);
        w.Write(bitsPerSample);
        w.Write(Encoding.ASCII.GetBytes("data"));
        w.Write(pcm.Length);
        w.Write(pcm);
        return ms.ToArray();
    }

    public void Dispose()
    {
        try
        {
            _audioSocket.AudioMediaReceived -= OnAudioReceived;
            _audioSocket.AudioSendStatusChanged -= OnSendStatusChanged;
        }
        catch { /* ignore */ }
    }
}
