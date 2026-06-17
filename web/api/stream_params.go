package api

import apigen "github.com/21S1298001/Mahiron5/web/api/gen"

func shouldDecode(decode apigen.OptInt) bool {
	value, ok := decode.Get()
	if !ok {
		return true
	}
	return value != 0
}
