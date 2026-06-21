package ts

import (
	"encoding/json"
	"time"
)

type eitSectionJSON struct {
	OriginalNetworkID        uint16         `json:"originalNetworkId"`
	TransportStreamID        uint16         `json:"transportStreamId"`
	ServiceID                uint16         `json:"serviceId"`
	TableID                  uint8          `json:"tableId"`
	SectionNumber            uint8          `json:"sectionNumber"`
	LastSectionNumber        uint8          `json:"lastSectionNumber"`
	SegmentLastSectionNumber uint8          `json:"segmentLastSectionNumber"`
	VersionNumber            uint8          `json:"versionNumber"`
	Events                   []eitEventJSON `json:"events"`
}

type eitEventJSON struct {
	EventID     uint16              `json:"eventId"`
	StartTime   int64               `json:"startTime"`
	Duration    int                 `json:"duration"`
	Scrambled   bool                `json:"scrambled"`
	Descriptors []eitDescriptorJSON `json:"descriptors"`
}

type eitDescriptorJSON struct {
	Type              string         `json:"$type"`
	EventName         string         `json:"eventName,omitempty"`
	Text              string         `json:"text,omitempty"`
	StreamContent     *int           `json:"streamContent,omitempty"`
	ComponentType     *int           `json:"componentType,omitempty"`
	ComponentTag      *int           `json:"componentTag,omitempty"`
	MainComponent     *bool          `json:"mainComponent,omitempty"`
	SamplingRate      *int           `json:"samplingRate,omitempty"`
	Lang              string         `json:"lang,omitempty"`
	Lang2             string         `json:"lang2,omitempty"`
	Nibbles           [][]int        `json:"nibbles,omitempty"`
	Items             [][]string     `json:"items,omitempty"`
	GroupType         *int           `json:"groupType,omitempty"`
	Events            []relatedEvent `json:"events,omitempty"`
	SeriesID          *int           `json:"seriesId,omitempty"`
	RepeatLabel       *int           `json:"repeatLabel,omitempty"`
	ProgramPattern    *int           `json:"programPattern,omitempty"`
	ExpireDate        *int64         `json:"expireDate,omitempty"`
	EpisodeNumber     *int           `json:"episodeNumber,omitempty"`
	LastEpisodeNumber *int           `json:"lastEpisodeNumber,omitempty"`
	SeriesName        string         `json:"seriesName,omitempty"`
}

type relatedEvent struct {
	OriginalNetworkID *uint16 `json:"originalNetworkId,omitempty"`
	TransportStreamID *uint16 `json:"transportStreamId,omitempty"`
	ServiceID         uint16  `json:"serviceId"`
	EventID           uint16  `json:"eventId"`
}

func eitToJSONSection(eit *EIT) eitSectionJSON {
	out := eitSectionJSON{
		OriginalNetworkID:        eit.OriginalNetworkID,
		TransportStreamID:        eit.TransportStreamID,
		ServiceID:                eit.ServiceID,
		TableID:                  eit.TableID,
		SectionNumber:            eit.SectionNumber,
		LastSectionNumber:        eit.LastSectionNumber,
		SegmentLastSectionNumber: eit.SegmentLastSectionNumber,
		VersionNumber:            eit.VersionNumber,
		Events:                   make([]eitEventJSON, 0, len(eit.Events)),
	}
	for _, event := range eit.Events {
		item := eitEventJSON{
			EventID:     event.EventID,
			StartTime:   unixMilli(event.StartTime),
			Duration:    int(event.Duration / time.Millisecond),
			Scrambled:   event.FreeCAMode,
			Descriptors: descriptorsToJSON(event.Descriptors),
		}
		out.Events = append(out.Events, item)
	}
	return out
}

func marshalEITJSONLine(eit *EIT) ([]byte, error) {
	data, err := json.Marshal(eitToJSONSection(eit))
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func unixMilli(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func descriptorsToJSON(descriptors []Descriptor) []eitDescriptorJSON {
	out := make([]eitDescriptorJSON, 0, len(descriptors))
	for _, desc := range descriptors {
		item, ok := descriptorToJSON(desc)
		if ok {
			out = append(out, item)
		}
	}
	return out
}

func descriptorToJSON(desc Descriptor) (eitDescriptorJSON, bool) {
	switch desc.Tag() {
	case DescriptorTagShortEvent:
		return parseShortEventDescriptor(desc)
	case DescriptorTagExtendedEvent:
		return parseExtendedEventDescriptor(desc)
	case DescriptorTagComponent:
		return parseComponentDescriptor(desc)
	case DescriptorTagContent:
		return parseContentDescriptor(desc)
	case DescriptorTagAudioComponent:
		return parseAudioComponentDescriptor(desc)
	case DescriptorTagSeries:
		return parseSeriesDescriptor(desc)
	case DescriptorTagEventGroup:
		return parseEventGroupDescriptor(desc)
	default:
		return eitDescriptorJSON{}, false
	}
}

func parseShortEventDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data) < 5 {
		return eitDescriptorJSON{}, false
	}
	lang := string(data[:3])
	nameLen := int(data[3])
	nameStart := 4
	nameEnd := nameStart + nameLen
	if nameEnd >= len(data) {
		return eitDescriptorJSON{}, false
	}
	textLen := int(data[nameEnd])
	textStart := nameEnd + 1
	textEnd := textStart + textLen
	if textEnd > len(data) {
		return eitDescriptorJSON{}, false
	}
	name, err := DecodeARIBString(data[nameStart:nameEnd])
	if err != nil {
		return eitDescriptorJSON{}, false
	}
	text, err := DecodeARIBString(data[textStart:textEnd])
	if err != nil {
		return eitDescriptorJSON{}, false
	}
	return eitDescriptorJSON{Type: "ShortEvent", Lang: lang, EventName: name, Text: text}, true
}

func parseExtendedEventDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data) < 6 {
		return eitDescriptorJSON{}, false
	}
	lang := string(data[1:4])
	itemsLen := int(data[4])
	off := 5
	itemsEnd := off + itemsLen
	if itemsEnd > len(data) {
		return eitDescriptorJSON{}, false
	}
	var items [][]string
	for off < itemsEnd {
		if off >= itemsEnd {
			return eitDescriptorJSON{}, false
		}
		descLen := int(data[off])
		off++
		if off+descLen > itemsEnd {
			return eitDescriptorJSON{}, false
		}
		itemDescription, err := DecodeARIBString(data[off : off+descLen])
		if err != nil {
			return eitDescriptorJSON{}, false
		}
		off += descLen
		if off >= itemsEnd {
			return eitDescriptorJSON{}, false
		}
		itemLen := int(data[off])
		off++
		if off+itemLen > itemsEnd {
			return eitDescriptorJSON{}, false
		}
		itemText, err := DecodeARIBString(data[off : off+itemLen])
		if err != nil {
			return eitDescriptorJSON{}, false
		}
		off += itemLen
		items = append(items, []string{itemDescription, itemText})
	}
	if off >= len(data) {
		return eitDescriptorJSON{}, false
	}
	textLen := int(data[off])
	off++
	if off+textLen > len(data) {
		return eitDescriptorJSON{}, false
	}
	text, err := DecodeARIBString(data[off : off+textLen])
	if err != nil {
		return eitDescriptorJSON{}, false
	}
	if text != "" && len(items) == 0 {
		items = append(items, []string{"", text})
	}
	return eitDescriptorJSON{Type: "ExtendedEvent", Lang: lang, Text: text, Items: items}, true
}

func parseContentDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data)%2 != 0 {
		return eitDescriptorJSON{}, false
	}
	nibbles := make([][]int, 0, len(data)/2)
	for i := 0; i+1 < len(data); i += 2 {
		nibbles = append(nibbles, []int{
			int(data[i] >> 4),
			int(data[i] & 0x0f),
			int(data[i+1] >> 4),
			int(data[i+1] & 0x0f),
		})
	}
	return eitDescriptorJSON{Type: "Content", Nibbles: nibbles}, true
}

func parseComponentDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data) < 6 {
		return eitDescriptorJSON{}, false
	}
	streamContent := int(data[0] & 0x0f)
	componentType := int(data[1])
	componentTag := int(data[2])
	text, err := DecodeARIBString(data[6:])
	if err != nil {
		return eitDescriptorJSON{}, false
	}
	return eitDescriptorJSON{
		Type:          "Component",
		StreamContent: intPtr(streamContent),
		ComponentType: intPtr(componentType),
		ComponentTag:  intPtr(componentTag),
		Lang:          string(data[3:6]),
		Text:          text,
	}, true
}

func parseAudioComponentDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data) < 9 {
		return eitDescriptorJSON{}, false
	}
	multilingual := data[5]&0x80 != 0
	main := data[5]&0x40 != 0
	samplingRate := int((data[5] >> 1) & 0x07)
	off := 9
	item := eitDescriptorJSON{
		Type:          "AudioComponent",
		StreamContent: intPtr(int(data[0] & 0x0f)),
		ComponentType: intPtr(int(data[1])),
		ComponentTag:  intPtr(int(data[2])),
		MainComponent: boolPtr(main),
		SamplingRate:  intPtr(samplingRate),
		Lang:          string(data[6:9]),
	}
	if multilingual {
		if len(data) < 12 {
			return eitDescriptorJSON{}, false
		}
		item.Lang2 = string(data[9:12])
		off = 12
	}
	text, err := DecodeARIBString(data[off:])
	if err != nil {
		return eitDescriptorJSON{}, false
	}
	item.Text = text
	return item, true
}

func parseSeriesDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data) < 8 {
		return eitDescriptorJSON{}, false
	}
	expireValid := data[2]&0x01 != 0
	episode := int(uint16(data[5])<<4 | uint16(data[6]>>4))
	lastEpisode := int(uint16(data[6]&0x0f)<<8 | uint16(data[7]))
	seriesName, err := DecodeARIBString(data[8:])
	if err != nil {
		return eitDescriptorJSON{}, false
	}
	item := eitDescriptorJSON{
		Type:              "Series",
		SeriesID:          intPtr(int(uint16(data[0])<<8 | uint16(data[1]))),
		RepeatLabel:       intPtr(int(data[2] >> 4)),
		ProgramPattern:    intPtr(int((data[2] >> 1) & 0x07)),
		EpisodeNumber:     intPtr(episode),
		LastEpisodeNumber: intPtr(lastEpisode),
		SeriesName:        seriesName,
	}
	if expireValid {
		t, err := parseMJDDate(data[3:5])
		if err != nil {
			return eitDescriptorJSON{}, false
		}
		v := t.UnixMilli()
		item.ExpireDate = &v
	}
	return item, true
}

func parseEventGroupDescriptor(desc Descriptor) (eitDescriptorJSON, bool) {
	data := desc.Data()
	if len(data) < 1 {
		return eitDescriptorJSON{}, false
	}
	groupType := int(data[0] >> 4)
	eventCount := int(data[0] & 0x0f)
	off := 1
	events := make([]relatedEvent, 0, eventCount)
	for i := 0; i < eventCount; i++ {
		if off+4 > len(data) {
			return eitDescriptorJSON{}, false
		}
		events = append(events, relatedEvent{
			ServiceID: uint16(data[off])<<8 | uint16(data[off+1]),
			EventID:   uint16(data[off+2])<<8 | uint16(data[off+3]),
		})
		off += 4
	}
	if groupType == 4 || groupType == 5 {
		for off+8 <= len(data) {
			onid := uint16(data[off])<<8 | uint16(data[off+1])
			tsid := uint16(data[off+2])<<8 | uint16(data[off+3])
			events = append(events, relatedEvent{
				OriginalNetworkID: uint16Ptr(onid),
				TransportStreamID: uint16Ptr(tsid),
				ServiceID:         uint16(data[off+4])<<8 | uint16(data[off+5]),
				EventID:           uint16(data[off+6])<<8 | uint16(data[off+7]),
			})
			off += 8
		}
		if off != len(data) {
			return eitDescriptorJSON{}, false
		}
	}
	return eitDescriptorJSON{Type: "EventGroup", GroupType: intPtr(groupType), Events: events}, true
}

func parseMJDDate(b []byte) (time.Time, error) {
	if len(b) != 2 {
		return time.Time{}, ErrInvalidSection
	}
	return parseMJDTime([]byte{b[0], b[1], 0, 0, 0})
}

func intPtr(v int) *int { return &v }

func boolPtr(v bool) *bool { return &v }

func uint16Ptr(v uint16) *uint16 { return &v }
