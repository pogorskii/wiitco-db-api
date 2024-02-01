package main

import (
	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.GET("/games/update", handleGamesUpdateRequest)
	router.GET("/movies/update", handleMoviesUpdateRequest)
	router.GET("/tvshows/update", handleTVShowsUpdateRequest)
	router.Run("localhost:8080")
}
