package ts

import "fmt"

// Descriptor represents a raw MPEG-2 descriptor.
type Descriptor []byte

// Tag returns the descriptor_tag.
func (d Descriptor) Tag() byte { return d[0] }

// Length returns the descriptor_length.
func (d Descriptor) Length() int { return int(d[1]) }

// Data returns the descriptor payload bytes (after tag and length).
func (d Descriptor) Data() []byte { return d[2 : 2+d.Length()] }

// ParseDescriptors parses a sequence of descriptors from bytes.
func ParseDescriptors(b []byte) []Descriptor {
	var descriptors []Descriptor
	for len(b) >= 2 {
		length := int(b[1])
		if len(b) < 2+length {
			break
		}
		descriptors = append(descriptors, Descriptor(b[:2+length]))
		b = b[2+length:]
	}
	return descriptors
}

// ARIB descriptor tags.
const (
	DescriptorTagCA                  = 0x09
	DescriptorTagService             = 0x48
	DescriptorTagServiceList         = 0x41
	DescriptorTagShortEvent          = 0x4D
	DescriptorTagExtendedEvent       = 0x4E
	DescriptorTagComponent           = 0x50
	DescriptorTagContent             = 0x54
	DescriptorTagAudioComponent      = 0xC4
	DescriptorTagExtendedBroadcaster = 0xCE
	DescriptorTagBroadcasterName     = 0xD8
	DescriptorTagDownloadContent     = 0xC9
	DescriptorTagLogoTransmission    = 0xCF
	// TS information descriptor carries remote_control_key_id in terrestrial NIT TS loops.
	DescriptorTagTSInformation = 0xCD
	// Terrestrial delivery system descriptor is assigned by the terrestrial operating guidelines.
	DescriptorTagTerrestrialDeliverySystem = 0xFA
	DescriptorTagEventGroup                = 0xD6
	DescriptorTagSeries                    = 0xD5
)

type TSInformationTransmissionType struct {
	TransmissionTypeInfo byte
	ServiceIDs           []uint16
}

type TSInformationDescriptor struct {
	RemoteControlKeyID uint8
	TSName             string
	TransmissionTypes  []TSInformationTransmissionType
}

func ParseTSInformationDescriptor(d Descriptor) (*TSInformationDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagTSInformation {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data) < 2 {
		return nil, ErrInvalidSection
	}
	nameLen := int(data[1] >> 2)
	transmissionTypeCount := int(data[1] & 0x03)
	nameStart := 2
	nameEnd := nameStart + nameLen
	if nameEnd > len(data) {
		return nil, ErrInvalidSection
	}
	name, err := DecodeARIBString(data[nameStart:nameEnd])
	if err != nil {
		return nil, err
	}
	result := &TSInformationDescriptor{
		RemoteControlKeyID: data[0],
		TSName:             name,
		TransmissionTypes:  make([]TSInformationTransmissionType, 0, transmissionTypeCount),
	}
	off := nameEnd
	for range transmissionTypeCount {
		if off+2 > len(data) {
			return nil, ErrInvalidSection
		}
		item := TSInformationTransmissionType{TransmissionTypeInfo: data[off]}
		numServices := int(data[off+1])
		off += 2
		if off+numServices*2 > len(data) {
			return nil, ErrInvalidSection
		}
		item.ServiceIDs = make([]uint16, 0, numServices)
		for range numServices {
			item.ServiceIDs = append(item.ServiceIDs, uint16(data[off])<<8|uint16(data[off+1]))
			off += 2
		}
		result.TransmissionTypes = append(result.TransmissionTypes, item)
	}
	return result, nil
}

type ServiceListEntry struct {
	ServiceID   uint16
	ServiceType uint8
}

type ServiceListDescriptor struct {
	Services []ServiceListEntry
}

func ParseServiceListDescriptor(d Descriptor) (*ServiceListDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagServiceList {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data)%3 != 0 {
		return nil, ErrInvalidSection
	}
	result := &ServiceListDescriptor{
		Services: make([]ServiceListEntry, 0, len(data)/3),
	}
	for off := 0; off < len(data); off += 3 {
		result.Services = append(result.Services, ServiceListEntry{
			ServiceID:   uint16(data[off])<<8 | uint16(data[off+1]),
			ServiceType: data[off+2],
		})
	}
	return result, nil
}

type TerrestrialDeliverySystemDescriptor struct {
	AreaCode         uint16
	GuardInterval    byte
	TransmissionMode byte
	Frequencies      []uint16
}

func ParseTerrestrialDeliverySystemDescriptor(d Descriptor) (*TerrestrialDeliverySystemDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() {
		return nil, ErrInvalidSection
	}
	if d.Tag() != DescriptorTagTerrestrialDeliverySystem {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data) < 2 || len(data[2:])%2 != 0 {
		return nil, ErrInvalidSection
	}
	result := &TerrestrialDeliverySystemDescriptor{
		AreaCode:         uint16(data[0])<<4 | uint16(data[1]>>4),
		GuardInterval:    (data[1] >> 2) & 0x03,
		TransmissionMode: data[1] & 0x03,
		Frequencies:      make([]uint16, 0, len(data[2:])/2),
	}
	for off := 2; off < len(data); off += 2 {
		result.Frequencies = append(result.Frequencies, uint16(data[off])<<8|uint16(data[off+1]))
	}
	return result, nil
}
