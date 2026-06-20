package main

import (
	"context"
	"os"

	"github.com/21S1298001/Mahiron5/internal/app"
)

func main() {
	os.Exit(app.Run(context.Background()))
}
