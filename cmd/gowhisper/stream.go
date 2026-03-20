package main

///////////////////////////////////////////////////////////////////////////////

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	schema "github.com/mutablelogic/go-whisper/pkg/schema"

	websocket "github.com/coder/websocket"
	wsjson "github.com/coder/websocket/wsjson"
	portaudio "github.com/gordonklaus/portaudio"
)

///////////////////////////////////////////////////////////////////////////////

type StreamCommands struct {
	Stream StreamCommand `cmd:"" name:"stream" help:"Stream microphone audio for real-time transcription." group:"TRANSCRIBE & TRANSLATE"`
}

type StreamCommand struct {
	Model       string   `arg:"" name:"model" help:"Model ID to use for transcription"`
	Language    *string  `name:"language" help:"Language code (e.g., 'en', 'es', 'fr')"`
	Prompt      *string  `name:"prompt" help:"Initial prompt to guide transcription"`
	Temperature *float64 `name:"temperature" help:"Temperature (0.0-1.0)"`
	Translate   bool     `name:"translate" help:"Translate to English"`
	StepMs       int     `name:"step-ms" help:"Processing step size in ms" default:"3000"`
	LengthMs     int     `name:"length-ms" help:"Processing window length in ms" default:"10000"`
	KeepMs       int     `name:"keep-ms" help:"Overlap to keep between windows in ms" default:"200"`
	VadThreshold float32 `name:"vad-threshold" help:"RMS energy threshold for voice activity detection (0 = disabled)" default:"0"`
}

///////////////////////////////////////////////////////////////////////////////

func (cmd *StreamCommand) Run(ctx *Globals) error {
	const framesPerBuffer = 1024

	// Build WebSocket URL from clientEndpoint
	endpoint, _, err := ctx.clientEndpoint("whisper")
	if err != nil {
		return err
	}
	wsURL := endpoint
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL = wsURL + "/stream"

	// Initialize PortAudio
	if err := portaudio.Initialize(); err != nil {
		return fmt.Errorf("portaudio initialize: %w", err)
	}
	defer portaudio.Terminate()

	// Open default input stream: 1 channel, 0 output, 16kHz, int16
	buf := make([]int16, framesPerBuffer)
	paStream, err := portaudio.OpenDefaultStream(1, 0, 16000, framesPerBuffer, buf)
	if err != nil {
		return fmt.Errorf("portaudio open stream: %w", err)
	}
	defer paStream.Close()

	// Connect via WebSocket
	conn, _, err := websocket.Dial(ctx.ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}
	defer conn.CloseNow()

	// Send StreamConfig
	config := schema.StreamConfig{
		Model:        cmd.Model,
		Language:     cmd.Language,
		Prompt:       cmd.Prompt,
		Temperature:  cmd.Temperature,
		Translate:    cmd.Translate,
		StepMs:       cmd.StepMs,
		LengthMs:     cmd.LengthMs,
		KeepMs:       cmd.KeepMs,
		VadThreshold: cmd.VadThreshold,
	}
	if err := wsjson.Write(ctx.ctx, conn, config); err != nil {
		return fmt.Errorf("send config: %w", err)
	}

	// Wait for ready event
	var readyEvent struct {
		Type string `json:"type"`
	}
	if err := wsjson.Read(ctx.ctx, conn, &readyEvent); err != nil {
		return fmt.Errorf("read ready event: %w", err)
	}
	if readyEvent.Type != "stream.ready" {
		return fmt.Errorf("expected stream.ready, got %q", readyEvent.Type)
	}

	// Start audio stream
	if err := paStream.Start(); err != nil {
		return fmt.Errorf("portaudio start stream: %w", err)
	}

	fmt.Fprintln(os.Stderr, "Listening... (press Ctrl+C to stop)")

	var wg sync.WaitGroup

	// Audio capture goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		pcm := make([]byte, framesPerBuffer*2)
		for {
			select {
			case <-ctx.ctx.Done():
				return
			default:
			}
			if err := paStream.Read(); err != nil {
				ctx.logger.Print(ctx.ctx, fmt.Sprintf("portaudio read: %v", err))
				return
			}
			for i, sample := range buf {
				binary.LittleEndian.PutUint16(pcm[i*2:], uint16(sample))
			}
			if err := conn.Write(ctx.ctx, websocket.MessageBinary, pcm); err != nil {
				if ctx.ctx.Err() == nil {
					ctx.logger.Print(ctx.ctx, fmt.Sprintf("websocket write: %v", err))
				}
				return
			}
		}
	}()

	// Event reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			var raw json.RawMessage
			if err := wsjson.Read(ctx.ctx, conn, &raw); err != nil {
				if ctx.ctx.Err() == nil {
					ctx.logger.Print(ctx.ctx, fmt.Sprintf("websocket read: %v", err))
				}
				return
			}
			var event struct {
				Type    string `json:"type"`
				Segment *struct {
					Text string `json:"text"`
				} `json:"segment,omitempty"`
				Error string `json:"error,omitempty"`
			}
			if err := json.Unmarshal(raw, &event); err != nil {
				ctx.logger.Print(ctx.ctx, fmt.Sprintf("unmarshal event: %v", err))
				continue
			}
			switch event.Type {
			case "stream.segment":
				if event.Segment != nil {
					fmt.Println(event.Segment.Text)
				}
			case "stream.done":
				return
			case "stream.error":
				fmt.Fprintf(os.Stderr, "stream error: %s\n", event.Error)
				return
			}
		}
	}()

	// Wait for context cancellation (Ctrl+C)
	<-ctx.ctx.Done()

	// Stop audio capture
	_ = paStream.Stop()

	// Send WebSocket close frame and wait for final segments
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	wg.Wait()
	return nil
}
