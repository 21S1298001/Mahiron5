package tuner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/util"
)

var newProcess = util.NewProcess

type TunerDeviceConfig struct {
	Channel        *config.ChannelConfig
	Command        string
	DecoderCommand string
}

type TunerDevice struct {
	channel        *config.ChannelConfig
	command        string
	decoderCommand string
	decoder        *util.Process
	done           chan struct{}
	err            error
	mu             sync.Mutex
	tunerProcess   *util.Process
}

func NewTunerDevice(config TunerDeviceConfig) *TunerDevice {
	return &TunerDevice{
		channel:        config.Channel,
		command:        config.Command,
		decoderCommand: config.DecoderCommand,
	}
}

func (d *TunerDevice) Start(ctx context.Context, dst io.Writer) error {
	d.mu.Lock()
	if d.done != nil {
		d.mu.Unlock()
		return nil
	}
	d.done = make(chan struct{})
	d.tunerProcess = newProcess(replaceCommandTemplate(d.command, d.channel))
	tunerOut, err := d.tunerProcess.StdoutPipe()
	if err != nil {
		d.done = nil
		d.mu.Unlock()
		return err
	}

	if d.decoderCommand != "" {
		d.decoder = newProcess(d.decoderCommand)
		d.decoder.Stdin(tunerOut)
		d.decoder.Stdout(dst)
		if err := d.decoder.Start(); err != nil {
			d.done = nil
			d.decoder = nil
			d.mu.Unlock()
			return err
		}
		if err := d.tunerProcess.Start(); err != nil {
			decoder := d.decoder
			d.done = nil
			d.decoder = nil
			d.tunerProcess = nil
			d.mu.Unlock()
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = decoder.Stop(stopCtx)
			return err
		}
		d.mu.Unlock()
		go d.waitDecoded()
		go d.stopOnContext(ctx)
		return nil
	}

	if err := d.tunerProcess.Start(); err != nil {
		d.done = nil
		d.tunerProcess = nil
		d.mu.Unlock()
		return err
	}
	d.mu.Unlock()
	go d.copyRaw(tunerOut, dst)
	go d.stopOnContext(ctx)
	return nil
}

func (d *TunerDevice) Done() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done
}

func (d *TunerDevice) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

func (d *TunerDevice) Stop(ctx context.Context) error {
	d.mu.Lock()
	tunerProcess := d.tunerProcess
	decoder := d.decoder
	done := d.done
	d.mu.Unlock()

	var result error
	if tunerProcess != nil {
		result = errors.Join(result, tunerProcess.Stop(ctx))
	}
	if decoder != nil {
		result = errors.Join(result, decoder.Stop(ctx))
	}
	if done != nil {
		select {
		case <-done:
			if err := d.Err(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
				result = errors.Join(result, err)
			}
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
	}
	return result
}

var commandTemplatePattern = regexp.MustCompile(`(?i)<([a-z0-9_.-]+)>`)

func replaceCommandTemplate(template string, channel *config.ChannelConfig) string {
	if channel == nil {
		return commandTemplatePattern.ReplaceAllString(template, "")
	}

	vars := map[string]any{
		"channel":  channel.Channel,
		"type":     channel.Type,
		"satelite": "",
		"space":    0,
	}
	if satellite, ok := channel.CommandVars["satellite"]; ok {
		vars["satelite"] = satellite
	}
	for key, value := range channel.CommandVars {
		vars[key] = value
	}

	return commandTemplatePattern.ReplaceAllStringFunc(template, func(match string) string {
		submatches := commandTemplatePattern.FindStringSubmatch(match)
		if len(submatches) != 2 {
			return ""
		}
		if value, ok := vars[submatches[1]]; ok {
			return fmt.Sprint(value)
		}
		return ""
	})
}

func (d *TunerDevice) copyRaw(src io.Reader, dst io.Writer) {
	_, err := io.Copy(dst, src)
	if err == nil || errors.Is(err, io.ErrClosedPipe) {
		d.finish(nil)
		return
	}
	d.finish(err)
}

func (d *TunerDevice) waitDecoded() {
	err := d.decoder.Wait()
	if err == nil {
		err = d.tunerProcess.Wait()
	}
	if err == nil || errors.Is(err, io.ErrClosedPipe) {
		d.finish(nil)
		return
	}
	d.finish(err)
}

func (d *TunerDevice) stopOnContext(ctx context.Context) {
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.Stop(stopCtx)
}

func (d *TunerDevice) finish(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done == nil {
		return
	}
	select {
	case <-d.done:
		return
	default:
		d.err = err
		close(d.done)
	}
}
