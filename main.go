package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/", handleRootRequest)
	router.GET("/games/update", handleGamesUpdateRequest)
	router.GET("/movies/update", handleMoviesUpdateRequest)
	router.GET("/tvshows/update", handleTVShowsUpdateRequest)
	router.Run("localhost:8080")
}

func handleRootRequest(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, "Hello, the API works")
}
