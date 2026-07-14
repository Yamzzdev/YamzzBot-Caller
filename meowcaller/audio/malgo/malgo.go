// Package malgo provides CGO-backed microphone and speaker devices that implement
// the meowcaller AudioSource and AudioSink interfaces. It is a separate Go module so
// the pure-Go meowcaller core stays cgo-free; consumers opt in by importing it.
//
// Devices speak the codec's native format directly — 16 kHz mono — so no resampling
// is needed. The underlying miniaudio backend (CoreAudio / WASAPI / ALSA /
// PulseAudio) is chosen by the OS.
package malgo

import (
	"encoding/binary"
	"sync"

	"github.com/gen2brain/malgo"
	meowcaller "github.com/purpshell/meowcaller"
)

const numChannels = 1

// Mic captures the OS default microphone as 16 kHz mono frames of
// meowcaller.FrameSamples and returns it as an AudioSource. The device callback
// delivers arbitrary chunk sizes, so frames are re-windowed into exact 60 ms frames;
// if the consumer falls behind, frames are dropped to keep the call real-time rather
// than accumulating latency.
func Mic() (meowcaller.AudioSource, error) {
	ctx, err := malgo.InitContext(nil, malgo.ContextConfig{}, nil)
	if err != nil {
		return nil, err
	}

	cfg := malgo.DefaultDeviceConfig(malgo.Capture)
	cfg.Capture.Format = malgo.FormatS16
	cfg.Capture.Channels = numChannels
	cfg.SampleRate = meowcaller.SampleRate
	cfg.Alsa.NoMMap = 1

	frames := make(chan []int16, 16)
	var acc []int16 // touched only by the (single) capture callback thread
	onData := func(_, in []byte, count uint32) {
		for i := 0; i+1 < len(in); i += 2 {
			acc = append(acc, int16(binary.LittleEndian.Uint16(in[i:])))
		}
		for len(acc) >= meowcaller.FrameSamples {
			f := make([]int16, meowcaller.FrameSamples)
			copy(f, acc[:meowcaller.FrameSamples])
			acc = acc[meowcaller.FrameSamples:]
			select {
			case frames <- f:
			default: // consumer slow: drop to stay real-time
			}
		}
	}

	dev, err := malgo.InitDevice(ctx.Context, cfg, malgo.DeviceCallbacks{Data: onData})
	if err != nil {
		_ = ctx.Uninit()
		ctx.Free()
		return nil, err
	}
	if err := dev.Start(); err != nil {
		dev.Uninit()
		_ = ctx.Uninit()
		ctx.Free()
		return nil, err
	}

	return &micSource{ctx: ctx, dev: dev, frames: frames}, nil
}

type micSource struct {
	ctx    *malgo.AllocatedContext
	dev    *malgo.Device
	frames <-chan []int16

	once sync.Once
}

// ReadFrame blocks for the next captured frame and returns it as 16 kHz mono float32.
func (m *micSource) ReadFrame() ([]float32, error) {
	pcm, ok := <-m.frames
	if !ok {
		return nil, nil
	}
	return pcmToFloat(pcm), nil
}

// Close stops capture and releases the device and miniaudio context.
func (m *micSource) Close() error {
	m.once.Do(func() {
		_ = m.dev.Stop()
		m.dev.Uninit()
		_ = m.ctx.Uninit()
		m.ctx.Free()
	})
	return nil
}

// Speaker plays received 16 kHz mono frames to the OS default speaker and returns it
// as an AudioSink. A small jitter buffer rides out scheduling gaps; underruns are
// zero-filled (silence) rather than glitching.
func Speaker() (meowcaller.AudioSink, error) {
	ctx, err := malgo.InitContext(nil, malgo.ContextConfig{}, nil)
	if err != nil {
		return nil, err
	}

	cfg := malgo.DefaultDeviceConfig(malgo.Playback)
	cfg.Playback.Format = malgo.FormatS16
	cfg.Playback.Channels = numChannels
	cfg.SampleRate = meowcaller.SampleRate

	in := make(chan []int16, 64)
	var (
		mu  sync.Mutex
		buf []int16
	)
	done := make(chan struct{})
	go func() {
		for f := range in {
			mu.Lock()
			buf = append(buf, f...)
			mu.Unlock()
		}
		close(done)
	}()

	onData := func(out, _ []byte, count uint32) {
		need := int(count)
		mu.Lock()
		n := min(need, len(buf))
		for i := range n {
			binary.LittleEndian.PutUint16(out[i*2:], uint16(buf[i]))
		}
		buf = buf[n:]
		mu.Unlock()
		for i := n * 2; i < need*2; i++ {
			out[i] = 0 // zero-fill underrun
		}
	}

	dev, err := malgo.InitDevice(ctx.Context, cfg, malgo.DeviceCallbacks{Data: onData})
	if err != nil {
		_ = ctx.Uninit()
		ctx.Free()
		return nil, err
	}
	if err := dev.Start(); err != nil {
		dev.Uninit()
		_ = ctx.Uninit()
		ctx.Free()
		return nil, err
	}

	return &speakerSink{ctx: ctx, dev: dev, in: in, done: done}, nil
}

type speakerSink struct {
	ctx  *malgo.AllocatedContext
	dev  *malgo.Device
	in   chan []int16
	done chan struct{}

	once sync.Once
}

// WriteFrame enqueues one 16 kHz mono frame for playback.
func (s *speakerSink) WriteFrame(frame []float32) error {
	s.in <- floatToPCM(frame)
	return nil
}

// Close stops playback, drains the jitter buffer, and releases the device and context.
func (s *speakerSink) Close() error {
	s.once.Do(func() {
		close(s.in)
		<-s.done
		_ = s.dev.Stop()
		s.dev.Uninit()
		_ = s.ctx.Uninit()
		s.ctx.Free()
	})
	return nil
}

// pcmToFloat converts s16 mono samples to the codec's [-1, 1) float range.
func pcmToFloat(pcm []int16) []float32 {
	out := make([]float32, len(pcm))
	for i, s := range pcm {
		out[i] = float32(s) / 32768
	}
	return out
}

// floatToPCM converts the codec's float output back to clamped s16 mono.
func floatToPCM(f []float32) []int16 {
	out := make([]int16, len(f))
	for i, s := range f {
		v := s * 32768
		switch {
		case v > 32767:
			v = 32767
		case v < -32768:
			v = -32768
		}
		out[i] = int16(v)
	}
	return out
}
