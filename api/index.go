package handler

import (
	"fmt"
	"net/http"
)

func Index(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<h1>This is a GO RESTful API created to update WIITCO DB</h1>")
}
