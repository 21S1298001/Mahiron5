package ts

import "fmt"

// SDTService represents a service entry in an SDT.
type SDTService struct {
	ServiceID           uint16
	EITScheduleFlag     bool
	EITPresentFollowing bool
	RunningStatus       byte
	FreeCAMode          bool
	Descriptors         []Descriptor
}

// SDT represents a Service Description Table.
type SDT struct {
	TransportStreamID uint16
	OriginalNetworkID uint16
	VersionNumber     byte
	Services          []SDTService
}

// ParseSDT parses an SDT section.
func ParseSDT(s Section) (*SDT, error) {
	if len(s) < 15 || (s.TableID() != TableIDSDT0 && s.TableID() != TableIDSDT1) || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	header, err := ParseSectionHeader(s)
	if err != nil {
		return nil, err
	}
	sectionEnd := s.TotalLength() - 4
	sdt := &SDT{
		TransportStreamID: header.TransportStreamID,
		OriginalNetworkID: uint16(s[8])<<8 | uint16(s[9]),
		VersionNumber:     header.VersionNumber,
	}
	for off := 11; off+5 <= sectionEnd; {
		descriptorsLoopLen := int(uint16(s[off+3]&0x0f)<<8 | uint16(s[off+4]))
		descStart := off + 5
		descEnd := descStart + descriptorsLoopLen
		if descEnd > sectionEnd {
			return nil, ErrInvalidSection
		}
		descriptors := append([]byte(nil), s[descStart:descEnd]...)
		sdt.Services = append(sdt.Services, SDTService{
			ServiceID:           uint16(s[off])<<8 | uint16(s[off+1]),
			EITScheduleFlag:     s[off+2]&0x02 != 0,
			EITPresentFollowing: s[off+2]&0x01 != 0,
			RunningStatus:       (s[off+3] >> 5) & 0x07,
			FreeCAMode:          s[off+3]&0x10 != 0,
			Descriptors:         ParseDescriptors(descriptors),
		})
		off = descEnd
	}
	return sdt, nil
}

type ServiceDescriptor struct {
	ServiceType         uint8
	ServiceProviderName string
	ServiceName         string
}

func ParseServiceDescriptor(d Descriptor) (*ServiceDescriptor, error) {
	if d.Tag() != DescriptorTagService {
		return nil, fmt.Errorf("ts: unexpected descriptor tag %#02x", d.Tag())
	}
	data := d.Data()
	if len(data) < 3 {
		return nil, ErrInvalidSection
	}
	providerLen := int(data[1])
	providerStart := 2
	providerEnd := providerStart + providerLen
	if providerEnd >= len(data) {
		return nil, ErrInvalidSection
	}
	nameLen := int(data[providerEnd])
	nameStart := providerEnd + 1
	nameEnd := nameStart + nameLen
	if nameEnd > len(data) {
		return nil, ErrInvalidSection
	}
	provider, err := DecodeARIBString(data[providerStart:providerEnd])
	if err != nil {
		return nil, err
	}
	name, err := DecodeARIBString(data[nameStart:nameEnd])
	if err != nil {
		return nil, err
	}
	return &ServiceDescriptor{
		ServiceType:         data[0],
		ServiceProviderName: provider,
		ServiceName:         name,
	}, nil
}
