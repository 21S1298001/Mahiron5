package program

import "sort"

type Program struct {
	ID        int64
	EventID   uint16
	ServiceID uint16
	NetworkID uint16
	StartAt   int64
	Duration  int
	IsFree    bool

	Name        string
	Description string
	Genres      []Genre
	Video       *Video
	Audios      []Audio
}

type Genre struct {
	Lv1 int
	Lv2 int
	Un1 int
	Un2 int
}

type Video struct {
	StreamContent int
	ComponentType int
}

type Audio struct {
	ComponentType int
	ComponentTag  *int
	IsMain        *bool
	SamplingRate  *int
	Langs         []string
}

type Query struct {
	ID        *int64
	NetworkID *uint16
	ServiceID *uint16
	EventID   *uint16
}

func ProgramID(networkID, serviceID, eventID uint16) int64 {
	return int64(networkID)*10000000000 + int64(serviceID)*100000 + int64(eventID)
}

func Sort(programs []*Program) {
	sort.SliceStable(programs, func(i, j int) bool {
		if programs[i].StartAt != programs[j].StartAt {
			return programs[i].StartAt < programs[j].StartAt
		}
		return programs[i].ID < programs[j].ID
	})
}

func cloneProgram(p *Program) *Program {
	if p == nil {
		return nil
	}
	cloned := *p
	cloned.Genres = append([]Genre(nil), p.Genres...)
	if p.Video != nil {
		video := *p.Video
		cloned.Video = &video
	}
	cloned.Audios = make([]Audio, len(p.Audios))
	for i := range p.Audios {
		cloned.Audios[i] = p.Audios[i]
		cloned.Audios[i].Langs = append([]string(nil), p.Audios[i].Langs...)
		if p.Audios[i].ComponentTag != nil {
			v := *p.Audios[i].ComponentTag
			cloned.Audios[i].ComponentTag = &v
		}
		if p.Audios[i].IsMain != nil {
			v := *p.Audios[i].IsMain
			cloned.Audios[i].IsMain = &v
		}
		if p.Audios[i].SamplingRate != nil {
			v := *p.Audios[i].SamplingRate
			cloned.Audios[i].SamplingRate = &v
		}
	}
	return &cloned
}
