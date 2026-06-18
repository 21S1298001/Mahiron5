package ts

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
	// TODO: implement descriptor loop parsing.
	return nil
}

// ARIB descriptor tags.
const (
	DescriptorTagService          = 0x48
	DescriptorTagShortEvent       = 0x4D
	DescriptorTagExtendedEvent    = 0x4E
	DescriptorTagComponent        = 0x50
	DescriptorTagContent          = 0x54
	DescriptorTagAudioComponent   = 0xC4
	DescriptorTagEventGroup       = 0xD6
	DescriptorTagSeries           = 0xD5
)
