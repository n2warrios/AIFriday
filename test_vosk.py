from vosk import Model, KaldiRecognizer
import wave
import json

# Path to your Vosk model directory (download and extract from https://alphacephei.com/vosk/models)
MODEL_PATH = r"C:\Users\GENAISINCBPUSR9\Documents\RAG\vosk-model-small-en-us-0.15\vosk-model-small-en-us-0.15"
AUDIO_FILE = "your_audio_file.wav"  # Change this to your actual audio file
TRANSCRIPT_FILE = "vosk_transcript.txt"

# Open the audio file (must be mono, 16kHz WAV)
wf = wave.open(AUDIO_FILE, "rb")
if wf.getnchannels() != 1 or wf.getsampwidth() != 2 or wf.getframerate() != 16000:
    print("Audio file must be WAV format mono PCM, 16kHz.")
    exit(1)

model = Model(MODEL_PATH)
rec = KaldiRecognizer(model, wf.getframerate())

results = []
while True:
    data = wf.readframes(4000)
    if len(data) == 0:
        break
    if rec.AcceptWaveform(data):
        results.append(json.loads(rec.Result()))
results.append(json.loads(rec.FinalResult()))

# Print the transcript
transcript = " ".join([res.get("text", "") for res in results])
print(transcript)

# Save transcript to file
with open(TRANSCRIPT_FILE, "w", encoding="utf-8") as f:
    f.write(transcript)

print(f"Transcript saved to {TRANSCRIPT_FILE}")
