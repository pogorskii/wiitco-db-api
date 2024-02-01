package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/lib/pq"
	"golang.org/x/time/rate"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/gin-gonic/gin"
)

// JSON structs
type TVShow struct {
	ID                  uint32              `json:"id"`
	Name                string              `json:"name"`
	CreatedBy           []Creator           `json:"created_by"`
	EpisodeRunTimes     []int32             `json:"episode_run_time"`
	FirstAirDate        string              `json:"first_air_date"`
	LastAirDate         string              `json:"last_air_date"`
	Genres              []Genre             `json:"genres"`
	InProduction        bool                `json:"in_production"`
	Languages           []string            `json:"languages"`
	Networks            []Network           `json:"Networks"`
	OriginCountries     []string            `json:"origin_country"`
	OriginalLanguage    string              `json:"original_language"`
	OriginalName        string              `json:"original_name"`
	Popularity          float32             `json:"popularity"`
	PosterPath          *string             `json:"poster_path"`
	ProductionCountries []ProductionCountry `json:"production_countries"`
	Seasons             []TVSeason          `json:"seasons"`
	Status              string              `json:"status"`
	Type                string              `json:"type"`
	VoteAverage         float32             `json:"vote_average"`
}

type Creator struct {
	ID   uint32 `json:"id"`
	Name string `json:"name"`
}

type Network struct {
	ID       uint32  `json:"id"`
	Name     string  `json:"name"`
	LogoPath *string `json:"logo_path" gorm:"column:logoPath"`
}

// DB structs

type TVShowBase struct {
	ID               uint32
	Name             string
	EpisodeRunTimes  pq.Int32Array  `gorm:"type:integer[]; column:episodeRunTimes"`
	FirstAirDate     *string        `gorm:"column:firstAirDate"`
	LastAirDate      *string        `gorm:"column:lastAirDate"`
	InProduction     bool           `gorm:"column:inProduction"`
	Languages        pq.StringArray `gorm:"column:languages"`
	OriginalLanguage string         `gorm:"column:originalLanguage"`
	OriginalName     string         `gorm:"column:originalName"`
	Popularity       float32
	PosterPath       *string `gorm:"column:posterPath"`
	Status           string
	Type             string
	VoteAverage      float32 `gorm:"column:voteAverage"`
}

type TVSeason struct {
	ID           uint32  `json:"id"`
	Name         string  `json:"name"`
	SeasonNumber uint16  `json:"season_number" gorm:"column:seasonNumber"`
	PosterPath   *string `json:"poster_path" gorm:"column:posterPath"`
	AirDate      string  `json:"air_date" gorm:"column:airDate"`
	EpisodeCount *uint16 `json:"episode_count" gorm:"column:episodeCount"`
	VoteAverage  float32 `json:"vote_average" gorm:"column:voteAverage"`
}

type TVSeasonDB struct {
	ShowID       uint32  `gorm:"column:showId"`
	ID           uint32  `json:"id"`
	Name         string  `json:"name"`
	SeasonNumber uint16  `json:"season_number" gorm:"column:seasonNumber"`
	PosterPath   *string `json:"poster_path" gorm:"column:posterPath"`
	AirDate      *string `json:"air_date" gorm:"column:airDate"`
	EpisodeCount *uint16 `json:"episode_count" gorm:"column:episodeCount"`
	VoteAverage  float32 `json:"vote_average" gorm:"column:voteAverage"`
}

type TVShowGenre struct {
	ShowId  uint32 `gorm:"column:showId"`
	GenreId uint32 `gorm:"column:genreId"`
}

type TVShowCreator struct {
	ShowId    uint32 `gorm:"column:showId"`
	CreatorId uint32 `gorm:"column:creatorId"`
}

type TVShowNetwork struct {
	ShowId    uint32 `gorm:"column:showId"`
	NetworkId uint32 `gorm:"column:networkId"`
}

type TVShowOrigCountry struct {
	ShowId     uint32 `gorm:"column:showId"`
	CountryIso string `gorm:"column:countryIso"`
}

type TVShowProdCountry struct {
	ShowId     uint32 `gorm:"column:showId"`
	CountryIso string `gorm:"column:countryIso"`
}

type TVResponse struct {
	Results      []TVShowIndex `json:"results"`
	Page         uint16        `json:"page"`
	TotalPages   uint16        `json:"total_pages"`
	TotalResults uint16        `json:"total_results"`
}

type TVShowIndex struct {
	ID    uint32 `json:"id"`
	Adult bool   `json:"adult"`
}

var (
	televisionLimiter = rate.NewLimiter(rate.Every(time.Second/40), 1)
)

func fetchTVIndexData(PageNum uint16) ([]byte, error) {
	if err := televisionLimiter.Wait(context.Background()); err != nil {
		fmt.Printf("Rate limit exceeded for Page %d: %v\n", PageNum, err)
	}

	url := fmt.Sprintf("https://api.themoviedb.org/3/tv/changes?page=%d", PageNum)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("API_ACCESS_TOKEN"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status code: %d", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func fetchAndProcessTVIndexData(pageNum uint16, idsCh chan uint32) {
	body, err := fetchTVIndexData(pageNum)
	if err != nil {
		fmt.Printf("Error fetching the first index page: %v\n", err)
		return
	}
	var rawInitData TVResponse
	err = json.Unmarshal(body, &rawInitData)
	if err != nil {
		fmt.Printf("Error unmarshalling the first index page: %v\n", err)
		return
	}
	if pageNum == 1 {
		totalPages = rawInitData.TotalPages
	}
	for _, entry := range rawInitData.Results {
		if !entry.Adult {
			idsCh <- entry.ID
		}
	}
}

func fetchTVDetailsData(id uint32) ([]byte, error) {
	if err := televisionLimiter.Wait(context.Background()); err != nil {
		fmt.Printf("Rate limit exceeded for Page %d: %v\n", id, err)
	}

	url := fmt.Sprintf("https://api.themoviedb.org/3/tv/%d?language=en-US", id)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("API_ACCESS_TOKEN"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status code: %d", res.StatusCode)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func fetchAndProcessTVDetailsData(id uint32, showBaseCh chan TVShowBase, seasonCh chan TVSeasonDB, genreCh chan TVShowGenre, creatorRefCh chan Creator, creatorCh chan TVShowCreator, networkRefCh chan Network, networkCh chan TVShowNetwork, origCountryCh chan TVShowOrigCountry, prodCountryCh chan TVShowProdCountry) {
	body, err := fetchTVDetailsData(id)
	if err != nil {
		fmt.Printf("Error fetching details for ID %d: %v\n", id, err)
		return
	}
	var show TVShow
	err = json.Unmarshal(body, &show)
	if err != nil {
		fmt.Println("Error parsing JSON data for Movie ID:", id, err)
		return
	}

	showBaseCh <- TVShowBase{
		ID:               show.ID,
		Name:             show.Name,
		EpisodeRunTimes:  show.EpisodeRunTimes,
		FirstAirDate:     filterEmptyDates(show.FirstAirDate),
		LastAirDate:      filterEmptyDates(show.LastAirDate),
		InProduction:     show.InProduction,
		Languages:        show.Languages,
		OriginalLanguage: show.OriginalLanguage,
		OriginalName:     show.OriginalName,
		Popularity:       show.Popularity,
		PosterPath:       show.PosterPath,
		Status:           show.Status,
		Type:             show.Type,
		VoteAverage:      show.VoteAverage,
	}

	for _, season := range show.Seasons {
		seasonCh <- TVSeasonDB{
			ShowID:       show.ID,
			ID:           id,
			Name:         season.Name,
			SeasonNumber: season.SeasonNumber,
			PosterPath:   season.PosterPath,
			AirDate:      filterEmptyDates(season.AirDate),
			EpisodeCount: season.EpisodeCount,
			VoteAverage:  season.VoteAverage,
		}
	}

	for _, genre := range show.Genres {
		legalGenres := [...]uint32{16, 18, 35, 37, 80, 99, 9648, 10751, 10759, 10762, 10763, 10764, 10765, 10766, 10767, 10768}

		genresSet := make(map[uint32]bool)
		for _, element := range legalGenres {
			genresSet[element] = true
		}

		if genresSet[uint32(genre.ID)] {
			genreCh <- TVShowGenre{
				ShowId:  show.ID,
				GenreId: uint32(genre.ID),
			}
		}
	}

	for _, creator := range show.CreatedBy {
		creatorRefCh <- creator

		creatorCh <- TVShowCreator{
			ShowId:    show.ID,
			CreatorId: creator.ID,
		}
	}

	for _, network := range show.Networks {
		networkRefCh <- network

		networkCh <- TVShowNetwork{
			ShowId:    show.ID,
			NetworkId: network.ID,
		}
	}

	for _, origCountry := range show.OriginCountries {
		origCountryCh <- TVShowOrigCountry{
			ShowId:     show.ID,
			CountryIso: origCountry,
		}
	}

	for _, prodCountry := range show.ProductionCountries {
		prodCountryCh <- TVShowProdCountry{
			ShowId:     show.ID,
			CountryIso: prodCountry.ISO31661,
		}
	}
}

func handleTVShowsUpdateRequest(c *gin.Context) {
	updateTVShows()
	c.IndentedJSON(http.StatusOK, "Finished updating tv shows DB")
}

func updateTVShows() {
	fmt.Printf("Started updating TV Shows at %s \n", time.Now().Format("15:04:05"))
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file:", err)
		return
	}

	username := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	database := os.Getenv("POSTGRES_DATABASE")
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=require TimeZone=Asia/Shanghai",
		host, username, password, database, port)
	db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
	}, nil)
	if err != nil {
		panic(err)
	}

	const batchSize = 500
	idsCh := make(chan uint32, 10000)
	showBaseCh := make(chan TVShowBase, 10000)
	seasonCh := make(chan TVSeasonDB, 100000)
	genreCh := make(chan TVShowGenre, 50000)
	creatorRefCh := make(chan Creator, 100000)
	creatorCh := make(chan TVShowCreator, 100000)
	networkRefCh := make(chan Network, 50000)
	networkCh := make(chan TVShowNetwork, 50000)
	origCountryCh := make(chan TVShowOrigCountry, 200000)
	prodCountryCh := make(chan TVShowProdCountry, 200000)

	var wgInit sync.WaitGroup
	wgInit.Add(1)
	go func() {
		defer wgInit.Done()
		fetchAndProcessTVIndexData(1, idsCh)
	}()
	wgInit.Wait()

	go func() {
		var wgFetch sync.WaitGroup
		var i uint16
		for i = 2; i <= totalPages; i++ {
			wgFetch.Add(1)
			go func(i uint16) {
				defer wgFetch.Done()
				fetchAndProcessTVIndexData(i, idsCh)
			}(i)
		}
		wgFetch.Wait()
		close(idsCh)
	}()

	go func() {
		var wgDetails sync.WaitGroup
		for id := range idsCh {
			wgDetails.Add(1)
			go func(id uint32) {
				defer wgDetails.Done()
				fetchAndProcessTVDetailsData(id, showBaseCh, seasonCh, genreCh, creatorRefCh, creatorCh, networkRefCh, networkCh, origCountryCh, prodCountryCh)
			}(id)
		}
		wgDetails.Wait()
		close(showBaseCh)
		close(seasonCh)
		close(genreCh)
		close(creatorCh)
		close(creatorRefCh)
		close(networkRefCh)
		close(networkCh)
		close(origCountryCh)
		close(prodCountryCh)
	}()

	var wgWriteBase sync.WaitGroup
	wgWriteBase.Add(1)
	go func() {
		defer wgWriteBase.Done()
		writeTVBaseRows(db, showBaseCh, batchSize)
	}()
	wgWriteBase.Add(1)
	go func() {
		defer wgWriteBase.Done()
		writeNetworkRefRows(db, networkRefCh, batchSize)
		writeCreatorRefRows(db, creatorRefCh, batchSize)
	}()
	wgWriteBase.Wait()

	var wgWriteChild sync.WaitGroup
	wgWriteChild.Add(1)
	go func() {
		defer wgWriteChild.Done()
		writeSeasonRows(db, seasonCh, batchSize)
	}()
	wgWriteChild.Wait()

	var wgWriteJoin sync.WaitGroup
	wgWriteJoin.Add(1)
	go func() {
		defer wgWriteJoin.Done()
		writeGenreRows(db, genreCh, batchSize)
		writeCreatorRows(db, creatorCh, batchSize)
		writeNetworkRows(db, networkCh, batchSize)
		writeOrigCountryRows(db, origCountryCh, batchSize)
		writeProdCountryRows(db, prodCountryCh, batchSize)
	}()
	wgWriteJoin.Wait()

	fmt.Println("Successfully fetched data and written to the DB")
}

func writeTVBaseRows(db *gorm.DB, dataChannel chan TVShowBase, batchSize int) {
	var batch []TVShowBase
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeTVBasesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVShowBase{}
		}
	}

	if len(batch) > 0 {
		if err := writeTVBasesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeTVBasesBatch(db *gorm.DB, objects []TVShowBase) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("TVShow").Model(&TVShowBase{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeSeasonRows(db *gorm.DB, dataChannel chan TVSeasonDB, batchSize int) {
	var batch []TVSeasonDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeSeasonsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVSeasonDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeSeasonsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeSeasonsBatch(db *gorm.DB, objects []TVSeasonDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVSeason").Model(&TVSeasonDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGenreRows(db *gorm.DB, dataChannel chan TVShowGenre, batchSize int) {
	var batch []TVShowGenre
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeTVGenresBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVShowGenre{}
		}
	}

	if len(batch) > 0 {
		if err := writeTVGenresBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeTVGenresBatch(db *gorm.DB, objects []TVShowGenre) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVShowGenre").Model(&TVShowGenre{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeCreatorRefRows(db *gorm.DB, dataChannel chan Creator, batchSize int) {
	var batch []Creator
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeCreatorRefsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []Creator{}
		}
	}

	if len(batch) > 0 {
		if err := writeCreatorRefsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeCreatorRefsBatch(db *gorm.DB, objects []Creator) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("CinemaPerson").Model(&Creator{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeCreatorRows(db *gorm.DB, dataChannel chan TVShowCreator, batchSize int) {
	var batch []TVShowCreator
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeCreatorsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVShowCreator{}
		}
	}

	if len(batch) > 0 {
		if err := writeCreatorsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeCreatorsBatch(db *gorm.DB, objects []TVShowCreator) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVShowCreator").Model(&TVShowCreator{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeNetworkRefRows(db *gorm.DB, dataChannel chan Network, batchSize int) {
	var batch []Network
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeNetworkRefsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []Network{}
		}
	}

	if len(batch) > 0 {
		if err := writeNetworkRefsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeNetworkRefsBatch(db *gorm.DB, objects []Network) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVNetwork").Model(&Network{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeNetworkRows(db *gorm.DB, dataChannel chan TVShowNetwork, batchSize int) {
	var batch []TVShowNetwork
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeNetworksBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVShowNetwork{}
		}
	}

	if len(batch) > 0 {
		if err := writeNetworksBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeNetworksBatch(db *gorm.DB, objects []TVShowNetwork) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVShowNetwork").Model(&TVShowNetwork{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeOrigCountryRows(db *gorm.DB, dataChannel chan TVShowOrigCountry, batchSize int) {
	var batch []TVShowOrigCountry
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeOrigCountriesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVShowOrigCountry{}
		}
	}

	if len(batch) > 0 {
		if err := writeOrigCountriesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeOrigCountriesBatch(db *gorm.DB, objects []TVShowOrigCountry) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVShowOrigCountry").Model(&TVShowOrigCountry{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeProdCountryRows(db *gorm.DB, dataChannel chan TVShowProdCountry, batchSize int) {
	var batch []TVShowProdCountry
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeProdCountriesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []TVShowProdCountry{}
		}
	}

	if len(batch) > 0 {
		if err := writeProdCountriesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeProdCountriesBatch(db *gorm.DB, objects []TVShowProdCountry) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("TVShowProdCountry").Model(&TVShowProdCountry{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}
