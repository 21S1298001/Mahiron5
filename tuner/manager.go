package tuner

type TunerManager struct {
	tuners []*Tuner
}

func NewTunerManager() *TunerManager {
	return &TunerManager{
		tuners: []*Tuner{},
	}
}
