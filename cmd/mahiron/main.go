package main

import (
	"context"
	"os"

	"github.com/21S1298001/mahiron/internal/app"
)

func main() {
	os.Exit(app.Run(context.Background(), os.Args[1:]))
}
