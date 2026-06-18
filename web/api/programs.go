package api

import (
	"context"
	"strconv"

	"github.com/21S1298001/Mahiron5/program"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func GetPrograms(ctx context.Context, h *Handler, params apigen.GetProgramsParams) (apigen.GetProgramsRes, error) {
	programs, err := h.programManager.List(ctx, programQuery(params))
	if err != nil {
		return nil, err
	}
	res := apigen.GetProgramsOKApplicationJSON(apiPrograms(programs))
	return &res, nil
}

func GetProgram(ctx context.Context, h *Handler, params apigen.GetProgramParams) (apigen.GetProgramRes, error) {
	p, ok, err := h.programManager.Get(ctx, params.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return notFound("program not found"), nil
	}
	return apiProgram(p), nil
}

func GetServicePrograms(ctx context.Context, h *Handler, params apigen.GetServiceProgramsParams) (apigen.GetServiceProgramsRes, error) {
	service, err := h.serviceManager.GetServiceById(ctx, strconv.FormatInt(params.ID, 10))
	if err != nil {
		return nil, err
	}
	if service == nil {
		return notFound("service not found"), nil
	}
	networkID := service.NetworkId
	serviceID := service.ServiceId
	programs, err := h.programManager.List(ctx, program.Query{
		NetworkID: &networkID,
		ServiceID: &serviceID,
	})
	if err != nil {
		return nil, err
	}
	res := apigen.GetServiceProgramsOKApplicationJSON(apiPrograms(programs))
	return &res, nil
}

func programQuery(params apigen.GetProgramsParams) program.Query {
	var query program.Query
	if value, ok := params.NetworkId.Get(); ok {
		v := uint16(value)
		query.NetworkID = &v
	}
	if value, ok := params.ServiceId.Get(); ok {
		v := uint16(value)
		query.ServiceID = &v
	}
	if value, ok := params.EventId.Get(); ok {
		v := uint16(value)
		query.EventID = &v
	}
	return query
}

func apiPrograms(programs []*program.Program) []apigen.Program {
	result := make([]apigen.Program, len(programs))
	for i, p := range programs {
		result[i] = *apiProgram(p)
	}
	return result
}

func apiProgram(p *program.Program) *apigen.Program {
	result := &apigen.Program{
		ID:           apigen.ProgramId(p.ID),
		EventId:      apigen.EventId(p.EventID),
		ServiceId:    apigen.ServiceId(p.ServiceID),
		NetworkId:    apigen.NetworkId(p.NetworkID),
		StartAt:      apigen.UnixtimeMS(p.StartAt),
		Duration:     p.Duration,
		IsFree:       p.IsFree,
		Genres:       apiProgramGenres(p.Genres),
		Audios:       apiProgramAudios(p.Audios),
		RelatedItems: []apigen.RelatedItem{},
	}
	if p.Name != "" {
		result.Name = apigen.NewOptString(p.Name)
	}
	if p.Description != "" {
		result.Description = apigen.NewOptString(p.Description)
	}
	if p.Video != nil {
		result.Video = apigen.NewOptProgramVideo(apigen.ProgramVideo{
			StreamContent: apigen.NewOptInt(p.Video.StreamContent),
			ComponentType: apigen.NewOptInt(p.Video.ComponentType),
		})
	}
	return result
}

func apiProgramGenres(genres []program.Genre) []apigen.ProgramGenre {
	result := make([]apigen.ProgramGenre, len(genres))
	for i, genre := range genres {
		result[i] = apigen.ProgramGenre{
			Lv1: apigen.NewOptInt(genre.Lv1),
			Lv2: apigen.NewOptInt(genre.Lv2),
			Un1: apigen.NewOptInt(genre.Un1),
			Un2: apigen.NewOptInt(genre.Un2),
		}
	}
	return result
}

func apiProgramAudios(audios []program.Audio) []apigen.ProgramAudiosItem {
	result := make([]apigen.ProgramAudiosItem, len(audios))
	for i, audio := range audios {
		item := apigen.ProgramAudiosItem{
			ComponentType: apigen.NewOptInt(audio.ComponentType),
			Langs:         apiProgramAudioLangs(audio.Langs),
		}
		if audio.ComponentTag != nil {
			item.ComponentTag = apigen.NewOptInt(*audio.ComponentTag)
		}
		if audio.IsMain != nil {
			item.IsMain = apigen.NewOptBool(*audio.IsMain)
		}
		if audio.SamplingRate != nil {
			item.SamplingRate = apigen.NewOptProgramAudioSamplingRate(apigen.ProgramAudioSamplingRate(*audio.SamplingRate))
		}
		result[i] = item
	}
	return result
}

func apiProgramAudioLangs(langs []string) []apigen.ProgramAudiosItemLangsItem {
	result := make([]apigen.ProgramAudiosItemLangsItem, 0, len(langs))
	for _, lang := range langs {
		switch lang {
		case "jpn", "eng", "deu", "fra", "ita", "rus", "zho", "kor", "spa":
			result = append(result, apigen.ProgramAudiosItemLangsItem(lang))
		case "etc":
			result = append(result, apigen.ProgramAudiosItemLangsItemEtc)
		}
	}
	return result
}
