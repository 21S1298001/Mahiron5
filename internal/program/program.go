package program

type Program struct {
	ID        int64  `json:"id"`
	EventID   uint16 `json:"eventId"`
	ServiceID uint16 `json:"serviceId"`
	NetworkID uint16 `json:"networkId"`
	StartAt   int64  `json:"startAt"`
	Duration  int    `json:"duration"`
	IsFree    bool   `json:"isFree"`

	Name         string            `json:"name,omitempty"`
	Description  string            `json:"description,omitempty"`
	Genres       []Genre           `json:"genres,omitempty"`
	Video        *Video            `json:"video,omitempty"`
	Audios       []Audio           `json:"audios,omitempty"`
	Extended     map[string]string `json:"extended,omitempty"`
	RelatedItems []RelatedItem     `json:"relatedItems,omitempty"`
	Series       *Series           `json:"series,omitempty"`
}

type Genre struct {
	Lv1 int `json:"lv1"`
	Lv2 int `json:"lv2"`
	Un1 int `json:"un1"`
	Un2 int `json:"un2"`
}

type Video struct {
	StreamContent int `json:"streamContent"`
	ComponentType int `json:"componentType"`
}

type Audio struct {
	ComponentType int      `json:"componentType"`
	ComponentTag  *int     `json:"componentTag,omitempty"`
	IsMain        *bool    `json:"isMain,omitempty"`
	SamplingRate  *int     `json:"samplingRate,omitempty"`
	Langs         []string `json:"langs,omitempty"`
}

type RelatedItem struct {
	Type              RelatedItemType `json:"type,omitempty"`
	NetworkID         *uint16         `json:"networkId,omitempty"`
	TransportStreamID *uint16         `json:"transportStreamId,omitempty"`
	ServiceID         uint16          `json:"serviceId,omitempty"`
	EventID           uint16          `json:"eventId,omitempty"`
}

type RelatedItemType string

const (
	RelatedItemTypeShared   RelatedItemType = "shared"
	RelatedItemTypeRelay    RelatedItemType = "relay"
	RelatedItemTypeMovement RelatedItemType = "movement"
)

type Series struct {
	ID          int    `json:"id"`
	Repeat      int    `json:"repeat"`
	Pattern     int    `json:"pattern"`
	ExpiresAt   *int64 `json:"expiresAt,omitempty"`
	Episode     int    `json:"episode"`
	LastEpisode int    `json:"lastEpisode"`
	Name        string `json:"name,omitempty"`
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
