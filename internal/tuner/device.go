package tuner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

type ProcessInfo struct {
	Command string
	PID     int
}

type ProcessStatus interface {
	ProcessStatus() ProcessInfo
}

type processDeviceConfig struct {
	Channel       *config.ChannelConfig
	Command       string
	DvbDevicePath string
}

type processDevice struct {
	channel         *config.ChannelConfig
	command         string
	done            chan struct{}
	err             error
	mu              sync.Mutex
	openAfterStart  func() (io.ReadCloser, error)
	openBeforeStart func(*util.Process) (io.ReadCloser, error)
	rawReader       io.Closer
	tunerProcess    *util.Process
}

type commandDevice struct {
	*processDevice
}

type dvbDevice struct {
	*processDevice
}

func NewCommandDevice(channel *config.ChannelConfig, command string) Device {
	return &commandDevice{
		processDevice: newProcessDevice(processDeviceConfig{
			Channel: channel,
			Command: command,
		}),
	}
}

func NewDVBDevice(channel *config.ChannelConfig, command, path string) Device {
	return &dvbDevice{
		processDevice: newProcessDevice(processDeviceConfig{
			Channel:       channel,
			Command:       command,
			DvbDevicePath: path,
		}),
	}
}

func newProcessDevice(config processDeviceConfig) *processDevice {
	device := &processDevice{
		channel: config.Channel,
		command: config.Command,
	}
	if config.DvbDevicePath == "" {
		device.openBeforeStart = func(process *util.Process) (io.ReadCloser, error) {
			return process.StdoutPipe()
		}
	} else {
		device.openAfterStart = func() (io.ReadCloser, error) {
			return os.Open(config.DvbDevicePath)
		}
	}
	return device
}

func (d *processDevice) Start(ctx context.Context, dst io.Writer) error {
	d.mu.Lock()
	if d.done != nil {
		d.mu.Unlock()
		return nil
	}
	d.done = make(chan struct{})
	d.tunerProcess = newProcess(replaceCommandTemplate(d.command, d.channel))
	var tunerOut io.ReadCloser
	var err error
	if d.openBeforeStart != nil {
		tunerOut, err = d.openBeforeStart(d.tunerProcess)
		if err != nil {
			d.done = nil
			d.mu.Unlock()
			return err
		}
	}

	if err := d.tunerProcess.Start(); err != nil {
		d.done = nil
		d.tunerProcess = nil
		d.mu.Unlock()
		return err
	}
	if d.openAfterStart != nil {
		tunerOut, err = d.openAfterStart()
		if err != nil {
			process := d.tunerProcess
			d.done = nil
			d.tunerProcess = nil
			d.mu.Unlock()
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return errors.Join(err, process.Stop(stopCtx))
		}
	}
	d.rawReader = tunerOut
	d.mu.Unlock()
	go d.copyRaw(tunerOut, dst)
	go d.stopOnContext(ctx)
	return nil
}

func (d *processDevice) Done() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done
}

func (d *processDevice) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

func (d *processDevice) ProcessStatus() ProcessInfo {
	d.mu.Lock()
	defer d.mu.Unlock()
	info := ProcessInfo{Command: replaceCommandTemplate(d.command, d.channel)}
	if d.tunerProcess == nil {
		return info
	}
	info.PID = d.tunerProcess.Pid()
	return info
}

func (d *processDevice) Stop(ctx context.Context) error {
	d.mu.Lock()
	tunerProcess := d.tunerProcess
	done := d.done
	d.mu.Unlock()

	var result error
	result = errors.Join(result, d.closeRawReader())
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

func (d *processDevice) copyRaw(src io.Reader, dst io.Writer) {
	defer func() { _ = d.closeRawReader() }()
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

func (d *processDevice) closeRawReader() error {
	d.mu.Lock()
	rawReader := d.rawReader
	d.rawReader = nil
	d.mu.Unlock()
	if rawReader == nil {
		return nil
	}
	return rawReader.Close()
}

func (d *processDevice) stopOnContext(ctx context.Context) {
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = d.Stop(stopCtx)
}

func (d *processDevice) finish(err error) {
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
