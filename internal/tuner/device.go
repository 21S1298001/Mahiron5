package tuner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/util"
)

var newProcess = util.NewProcess

type Device interface {
	Start(context.Context, io.Writer) error
	Stop(context.Context) error
	Done() <-chan struct{}
	Err() error
}

type TunerDeviceConfig struct {
	Channel *config.ChannelConfig
	Command string
}

type TunerDevice struct {
	channel      *config.ChannelConfig
	command      string
	done         chan struct{}
	err          error
	mu           sync.Mutex
	tunerProcess *util.Process
}

func NewTunerDevice(config TunerDeviceConfig) *TunerDevice {
	return &TunerDevice{
		channel: config.Channel,
		command: config.Command,
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

func (d *TunerDevice) Pid() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tunerProcess == nil {
		return 0
	}
	return d.tunerProcess.Pid()
}

func (d *TunerDevice) Command() string {
	return replaceCommandTemplate(d.command, d.channel)
}

func (d *TunerDevice) Stop(ctx context.Context) error {
	d.mu.Lock()
	tunerProcess := d.tunerProcess
	done := d.done
	d.mu.Unlock()

	var result error
	if tunerProcess != nil {
		result = errors.Join(result, tunerProcess.Stop(ctx))
	}
	if done != nil {
		select {
		case <-done:
			if err := d.Err(); err != nil && !util.IsExpectedStreamCloseError(err) {
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
	_, copyErr := io.Copy(dst, src)
	if util.IsExpectedStreamCloseError(copyErr) {
		copyErr = nil
	}
	d.mu.Lock()
	process := d.tunerProcess
	d.mu.Unlock()
	var waitErr error
	if process != nil {
		waitErr = process.Wait()
	}
	d.finish(errors.Join(copyErr, waitErr))
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
