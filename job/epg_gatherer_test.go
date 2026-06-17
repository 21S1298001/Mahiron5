package job

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/program"
)

func TestCollectEITSUntilCompleteWaitsForAllSections(t *testing.T) {
	manager := program.NewProgramManager(nil)
	sections := []string{
		`{"originalNetworkId":1,"transportStreamId":2,"serviceId":3,"tableId":80,"sectionNumber":0,"lastSectionNumber":1,"versionNumber":1,"events":[{"eventId":1,"startTime":1000,"duration":1000,"scrambled":false}]}`,
		`{"originalNetworkId":1,"transportStreamId":2,"serviceId":3,"tableId":80,"sectionNumber":1,"lastSectionNumber":1,"versionNumber":1,"events":[{"eventId":2,"startTime":2000,"duration":1000,"scrambled":false}]}`,
	}

	err := collectEITSUntilComplete(context.Background(), manager, func(ctx context.Context, dst io.Writer) error {
		for _, section := range sections {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if _, err := io.WriteString(dst, section+"\n"); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}

	serviceID := uint16(3)
	programs := manager.List(program.Query{ServiceID: &serviceID})
	if len(programs) != 2 {
		t.Fatalf("programs length = %d, want 2", len(programs))
	}
}

func TestCollectEITSUntilCompleteReportsEarlyEOF(t *testing.T) {
	manager := program.NewProgramManager(nil)
	err := collectEITSUntilComplete(context.Background(), manager, func(ctx context.Context, dst io.Writer) error {
		_, err := io.Copy(dst, strings.NewReader(`{"originalNetworkId":1,"transportStreamId":2,"serviceId":3,"tableId":80,"sectionNumber":0,"lastSectionNumber":1,"versionNumber":1,"events":[]}`+"\n"))
		return err
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "ended before all sections") {
		t.Fatalf("error = %v", err)
	}
}
