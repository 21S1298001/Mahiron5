package ts

import "fmt"

type BIT struct {
	OriginalNetworkID      uint16
	VersionNumber          byte
	CurrentNext            bool
	SectionNumber          byte
	LastSectionNumber      byte
	BroadcastViewPropriety bool
	FirstDescriptors       []Descriptor
	Broadcasters           []BITBroadcaster
}

type BITBroadcaster struct {
	BroadcasterID byte
	Descriptors   []Descriptor
}

type ExtendedBroadcasterDescriptor struct {
	BroadcasterType          byte
	TerrestrialBroadcasterID uint16
	AffiliationIDs           []byte
	Broadcasters             []AffiliatedBroadcaster
	PrivateData              []byte
}

type AffiliatedBroadcaster struct {
	OriginalNetworkID uint16
	BroadcasterID     byte
}

func ParseBIT(s Section) (*BIT, error) {
	if len(s) < 14 || s.TableID() != TableIDBIT || !s.SectionSyntaxIndicator() || s.TotalLength() > len(s) || !s.ValidateCRC() {
		return nil, ErrInvalidSection
	}
	end := s.TotalLength() - 4
	firstLen := int(uint16(s[8]&0x0f)<<8 | uint16(s[9]))
	firstEnd := 10 + firstLen
	if firstEnd > end {
		return nil, ErrInvalidSection
	}
	out := &BIT{
		OriginalNetworkID: uint16(s[3])<<8 | uint16(s[4]), VersionNumber: (s[5] >> 1) & 0x1f,
		CurrentNext: s[5]&1 != 0, SectionNumber: s[6], LastSectionNumber: s[7],
		BroadcastViewPropriety: s[8]&0x10 != 0, FirstDescriptors: ParseDescriptors(s[10:firstEnd]),
	}
	if descriptorsLength(out.FirstDescriptors) != firstLen {
		return nil, ErrInvalidSection
	}
	for off := firstEnd; off < end; {
		if off+3 > end {
			return nil, ErrInvalidSection
		}
		length := int(uint16(s[off+1]&0x0f)<<8 | uint16(s[off+2]))
		descEnd := off + 3 + length
		if descEnd > end {
			return nil, ErrInvalidSection
		}
		descriptors := ParseDescriptors(s[off+3 : descEnd])
		if descriptorsLength(descriptors) != length {
			return nil, ErrInvalidSection
		}
		out.Broadcasters = append(out.Broadcasters, BITBroadcaster{BroadcasterID: s[off], Descriptors: descriptors})
		off = descEnd
	}
	return out, nil
}

func ParseExtendedBroadcasterDescriptor(d Descriptor) (*ExtendedBroadcasterDescriptor, error) {
	if len(d) < 2 || len(d) < 2+d.Length() || d.Tag() != DescriptorTagExtendedBroadcaster {
		return nil, fmt.Errorf("ts: invalid extended broadcaster descriptor: %w", ErrInvalidSection)
	}
	data := d.Data()
	if len(data) < 1 {
		return nil, ErrInvalidSection
	}
	out := &ExtendedBroadcasterDescriptor{BroadcasterType: data[0] >> 4}
	if out.BroadcasterType != 1 && out.BroadcasterType != 2 {
		out.PrivateData = append([]byte(nil), data[1:]...)
		return out, nil
	}
	if len(data) < 4 {
		return nil, ErrInvalidSection
	}
	out.TerrestrialBroadcasterID = uint16(data[1])<<8 | uint16(data[2])
	affiliationCount, broadcasterCount := int(data[3]>>4), int(data[3]&0x0f)
	off := 4
	if off+affiliationCount+broadcasterCount*3 > len(data) {
		return nil, ErrInvalidSection
	}
	out.AffiliationIDs = append([]byte(nil), data[off:off+affiliationCount]...)
	off += affiliationCount
	for range broadcasterCount {
		out.Broadcasters = append(out.Broadcasters, AffiliatedBroadcaster{OriginalNetworkID: uint16(data[off])<<8 | uint16(data[off+1]), BroadcasterID: data[off+2]})
		off += 3
	}
	out.PrivateData = append([]byte(nil), data[off:]...)
	return out, nil
}

func ParseBroadcasterNameDescriptor(d Descriptor) (string, error) {
	if len(d) < 2 || len(d) < 2+d.Length() || d.Tag() != DescriptorTagBroadcasterName {
		return "", ErrInvalidSection
	}
	return DecodeARIBString(d.Data())
}

func descriptorsLength(descriptors []Descriptor) int {
	n := 0
	for _, d := range descriptors {
		n += len(d)
	}
	return n
}
