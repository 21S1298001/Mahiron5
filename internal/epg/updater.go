package epg

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/21S1298001/Mahiron5/internal/program"
)

type ProgramWriter interface {
	UpsertPrograms(context.Context, []*program.Program) error
}

type ProgramStore interface {
	ProgramWriter
	DeleteEndedBefore(context.Context, int64) error
	ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error
}

type Updater struct {
	store ProgramWriter
}

func NewUpdater(store ProgramWriter) *Updater {
	return &Updater{store: store}
}

func DecodeSectionJSON(data []byte) (*EITSection, error) {
	var section EITSection
	if err := json.Unmarshal(data, &section); err != nil {
		return nil, err
	}
	return &section, nil
}

func (u *Updater) UpsertEITSection(ctx context.Context, section *EITSection) error {
	return u.store.UpsertPrograms(ctx, section.Programs())
}

func (u *Updater) UpsertEITSectionJSON(ctx context.Context, data []byte) error {
	section, err := DecodeSectionJSON(data)
	if err != nil {
		return err
	}
	return u.UpsertEITSection(ctx, section)
}

func (u *Updater) ReadEITJSONL(ctx context.Context, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if err := u.UpsertEITSectionJSON(ctx, line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return ctx.Err()
}
