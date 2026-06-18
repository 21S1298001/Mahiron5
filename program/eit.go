package program

import "encoding/json"

type EITSection struct {
	OriginalNetworkID        uint16     `json:"originalNetworkId"`
	TransportStreamID        uint16     `json:"transportStreamId"`
	ServiceID                uint16     `json:"serviceId"`
	TableID                  uint8      `json:"tableId"`
	SectionNumber            uint8      `json:"sectionNumber"`
	LastSectionNumber        uint8      `json:"lastSectionNumber"`
	SegmentLastSectionNumber uint8      `json:"segmentLastSectionNumber"`
	VersionNumber            uint8      `json:"versionNumber"`
	Events                   []EITEvent `json:"events"`
}

type EITEvent struct {
	EventID     uint16          `json:"eventId"`
	StartTime   int64           `json:"startTime"`
	Duration    int             `json:"duration"`
	Scrambled   bool            `json:"scrambled"`
	Descriptors []EITDescriptor `json:"descriptors"`
}

type EITDescriptor struct {
	Type              string          `json:"$type"`
	EventName         string          `json:"eventName"`
	Text              string          `json:"text"`
	StreamContent     *int            `json:"streamContent"`
	ComponentType     *int            `json:"componentType"`
	ComponentTag      *int            `json:"componentTag"`
	MainComponent     *bool           `json:"mainComponent"`
	SamplingRate      *int            `json:"samplingRate"`
	Lang              string          `json:"lang"`
	Lang2             string          `json:"lang2"`
	Nibbles           [][]int         `json:"nibbles"`
	Items             [][]string      `json:"items"`
	GroupType         *int            `json:"groupType"`
	Events            []RelatedEvent  `json:"events"`
	SeriesID          *int            `json:"seriesId"`
	RepeatLabel       *int            `json:"repeatLabel"`
	ProgramPattern    *int            `json:"programPattern"`
	ExpireDate        *int64          `json:"expireDate"`
	EpisodeNumber     *int            `json:"episodeNumber"`
	LastEpisodeNumber *int            `json:"lastEpisodeNumber"`
	SeriesName        string          `json:"seriesName"`
	Raw               json.RawMessage `json:"-"`
}

type RelatedEvent struct {
	OriginalNetworkID *uint16 `json:"originalNetworkId"`
	TransportStreamID *uint16 `json:"transportStreamId"`
	ServiceID         uint16  `json:"serviceId"`
	EventID           uint16  `json:"eventId"`
}

func (d *EITDescriptor) UnmarshalJSON(data []byte) error {
	type descriptor EITDescriptor
	var decoded descriptor
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*d = EITDescriptor(decoded)
	var aliases struct {
		LanguageCode      string `json:"languageCode"`
		LanguageCode2     string `json:"languageCode2"`
		MainComponentFlag *bool  `json:"mainComponentFlag"`
	}
	if err := json.Unmarshal(data, &aliases); err != nil {
		return err
	}
	if d.Lang == "" {
		d.Lang = aliases.LanguageCode
	}
	if d.Lang2 == "" {
		d.Lang2 = aliases.LanguageCode2
	}
	if d.MainComponent == nil {
		d.MainComponent = aliases.MainComponentFlag
	}
	d.Raw = append(d.Raw[:0], data...)
	return nil
}

func (s *EITSection) Programs() []*Program {
	programs := make([]*Program, 0, len(s.Events))
	for _, event := range s.Events {
		program := &Program{
			ID:        ProgramID(s.OriginalNetworkID, s.ServiceID, event.EventID),
			EventID:   event.EventID,
			ServiceID: s.ServiceID,
			NetworkID: s.OriginalNetworkID,
			StartAt:   event.StartTime,
			Duration:  event.Duration,
			IsFree:    !event.Scrambled,
		}
		for _, descriptor := range event.Descriptors {
			applyDescriptor(program, descriptor)
		}
		programs = append(programs, program)
	}
	return programs
}

func applyDescriptor(program *Program, descriptor EITDescriptor) {
	switch descriptor.Type {
	case "ShortEvent":
		program.Name = descriptor.EventName
		program.Description = descriptor.Text
	case "Content":
		for _, nibble := range descriptor.Nibbles {
			if len(nibble) < 4 {
				continue
			}
			program.Genres = append(program.Genres, Genre{
				Lv1: nibble[0],
				Lv2: nibble[1],
				Un1: nibble[2],
				Un2: nibble[3],
			})
		}
	case "Component":
		video := &Video{}
		if descriptor.StreamContent != nil {
			video.StreamContent = *descriptor.StreamContent
		}
		if descriptor.ComponentType != nil {
			video.ComponentType = *descriptor.ComponentType
		}
		program.Video = video
	case "AudioComponent":
		audio := Audio{}
		if descriptor.ComponentType != nil {
			audio.ComponentType = *descriptor.ComponentType
		}
		if descriptor.ComponentTag != nil {
			v := *descriptor.ComponentTag
			audio.ComponentTag = &v
		}
		if descriptor.MainComponent != nil {
			v := *descriptor.MainComponent
			audio.IsMain = &v
		}
		if descriptor.SamplingRate != nil {
			v := samplingRate(*descriptor.SamplingRate)
			audio.SamplingRate = &v
		}
		audio.Langs = descriptorLangs(descriptor)
		program.Audios = append(program.Audios, audio)
	case "ExtendedEvent":
		if program.Extended == nil {
			program.Extended = make(map[string]string)
		}
		for _, item := range descriptor.Items {
			if len(item) >= 2 {
				program.Extended[item[0]] = item[1]
			}
		}
	case "EventGroup":
		var groupType RelatedItemType
		if descriptor.GroupType != nil {
			switch *descriptor.GroupType {
			case 0x01:
				groupType = RelatedItemTypeShared
			case 0x02:
				groupType = RelatedItemTypeRelay
			case 0x04:
				groupType = RelatedItemTypeMovement
			}
		}
		for _, event := range descriptor.Events {
			program.RelatedItems = append(program.RelatedItems, RelatedItem{
				Type:              groupType,
				NetworkID:         event.OriginalNetworkID,
				TransportStreamID: event.TransportStreamID,
				ServiceID:         event.ServiceID,
				EventID:           event.EventID,
			})
		}
	case "Series":
		series := &Series{Name: descriptor.SeriesName}
		if descriptor.SeriesID != nil {
			series.ID = *descriptor.SeriesID
		}
		if descriptor.RepeatLabel != nil {
			series.Repeat = *descriptor.RepeatLabel
		}
		if descriptor.ProgramPattern != nil {
			series.Pattern = *descriptor.ProgramPattern
		}
		if descriptor.ExpireDate != nil {
			v := *descriptor.ExpireDate
			series.ExpiresAt = &v
		}
		if descriptor.EpisodeNumber != nil {
			series.Episode = *descriptor.EpisodeNumber
		}
		if descriptor.LastEpisodeNumber != nil {
			series.LastEpisode = *descriptor.LastEpisodeNumber
		}
		program.Series = series
	}
}

func descriptorLangs(descriptor EITDescriptor) []string {
	langs := make([]string, 0, 2)
	if descriptor.Lang != "" {
		langs = append(langs, descriptor.Lang)
	}
	if descriptor.Lang2 != "" {
		langs = append(langs, descriptor.Lang2)
	}
	return langs
}

func samplingRate(code int) int {
	switch code {
	case 1:
		return 16000
	case 2:
		return 22050
	case 3:
		return 24000
	case 5:
		return 32000
	case 6:
		return 44100
	case 7:
		return 48000
	default:
		return code
	}
}
