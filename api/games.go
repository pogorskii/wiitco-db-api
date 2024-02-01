package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/lib/pq"
	"golang.org/x/time/rate"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Game struct {
	ID                    uint32             `json:"id"`
	Name                  string             `json:"name"`
	Slug                  string             `json:"slug"`
	AgeRatings            []AgeRating        `json:"age_ratings" gorm:"-"`
	AggregatedRating      float32            `json:"aggregated_rating" gorm:"column:rating"`
	AggregatedRatingCount uint32             `json:"aggregated_rating_count" gorm:"column:reviewsCount"`
	AlternativeNames      []AlternativeName  `json:"alternative_names" gorm:"-"`
	Category              uint8              `json:"category"`
	Collection            *Collection        `json:"collection"`
	Collections           []Collection       `json:"collections"`
	Cover                 *Cover             `json:"cover"`
	DLCs                  []uint32           `json:"dlcs"`
	ExpandedGames         []uint32           `json:"expanded_games"`
	Expansions            []uint32           `json:"expansions"`
	ExternalGames         []ExternalGame     `json:"external_games"`
	FirstReleaseDate      *uint32            `json:"first_release_date"`
	Follows               *uint32            `json:"follows"`
	Franchise             *Franchise         `json:"franchise"`
	Franchises            []Franchise        `json:"franchises"`
	GameEngines           []Engine           `json:"game_engines"`
	GameLocalizations     []GameLocalization `json:"game_localizations"`
	GameModes             []uint8            `json:"game_modes"`
	Genres                []uint8            `json:"genres"`
	Hypes                 *uint32            `json:"hypes"`
	InvolvedCompanies     []uint32           `json:"involved_companies"`
	LanguageSupports      []LanguageSupport  `json:"language_supports"`
	ParentGame            *uint32            `json:"parent_game"`
	Platforms             []uint16           `json:"platforms"`
	PlayerPerspectives    []uint16           `json:"player_perspectives"`
	Ports                 []uint32           `json:"ports"`
	ReleaseDates          []ReleaseDate      `json:"release_dates"`
	Remakes               []uint32           `json:"remakes"`
	Remasters             []uint32           `json:"remasters"`
	StandaloneExpansions  []uint32           `json:"standalone_expansions"`
	Screenshots           []Screenshot       `json:"screenshots"`
	SimilarGames          []uint32           `json:"similar_games"`
	Status                *uint8             `json:"status"`
	Summary               *string            `json:"summary"`
	Themes                []uint16           `json:"themes"`
	VersionParent         *uint32            `json:"version_parent"`
	VersionTitle          *string            `json:"version_title"`
	Videos                []Video            `json:"videos"`
	Websites              []Website          `json:"websites"`
	UpdatedAt             uint32             `json:"updated_at"`
	Checksum              string             `json:"checksum"`
}

type Collection struct {
	ID        uint32 `json:"id"`
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	TypeId    uint32 `json:"type"`
	UpdatedAt uint32 `json:"updated_at"`
	Checksum  string `json:"checksum"`
}

type Franchise struct {
	ID        uint32 `json:"id"`
	Name      string `json:"name"`
	Slug      string `json:"slug"`
	UpdatedAt uint32 `json:"updated_at"`
	Checksum  string `json:"checksum"`
}

type Engine struct {
	ID          uint32  `json:"id"`
	Name        string  `json:"name"`
	Slug        string  `json:"slug"`
	Description *string `json:"description"`
	UpdatedAt   uint32  `json:"updated_at"`
	Checksum    string  `json:"checksum"`
}

type AgeRating struct {
	ID                  uint32               `json:"id"`
	Category            uint16               `json:"category"`
	Rating              uint16               `json:"rating"`
	RatingCoverUrl      *string              `json:"rating_cover_url"`
	Synopsis            *string              `json:"synopsis"`
	Checksum            string               `json:"checksum"`
	ContentDescriptions []ContentDescription `json:"content_descriptions"`
}

type ContentDescription struct {
	ID          uint32 `json:"id"`
	Category    uint16 `json:"category"`
	Description string `json:"description"`
	Checksum    string `json:"checksum"`
}

type AlternativeName struct {
	ID       uint32  `json:"id"`
	Name     string  `json:"name"`
	Comment  *string `json:"comment"`
	Checksum string  `json:"checksum"`
}

type Cover struct {
	ID           uint32  `json:"id"`
	AlphaChannel bool    `json:"alpha_channel"`
	Animated     bool    `json:"animated"`
	ImageID      string  `json:"image_id"`
	Width        *uint16 `json:"width"`
	Height       *uint16 `json:"height"`
	Checksum     string  `json:"checksum"`
}

type ExternalGame struct {
	ID        uint32        `json:"id"`
	Name      *string       `json:"name"`
	Category  uint16        `json:"category"`
	Countries pq.Int32Array `json:"countries"`
	Media     *uint8        `json:"media"`
	Platform  *uint16       `json:"platform"`
	Url       *string       `json:"url"`
	UpdatedAt uint32        `json:"updated_at"`
	Checksum  string        `json:"checksum"`
}

type GameLocalization struct {
	ID        uint32  `json:"id"`
	Name      string  `json:"name"`
	Region    *uint16 `json:"region"`
	UpdatedAt uint32  `json:"updated_at"`
	Checksum  string  `json:"checksum"`
}

type LanguageSupport struct {
	ID                  uint32 `json:"id"`
	Language            uint8  `json:"language"`
	LanguageSupportType uint8  `json:"language_support_type"`
	UpdatedAt           uint32 `json:"updated_at"`
	Checksum            string `json:"checksum"`
}

type ReleaseDate struct {
	ID        uint32  `json:"id"`
	Category  uint8   `json:"category"`
	Date      *uint32 `json:"date"`
	Human     string  `json:"human"`
	Month     *uint8  `json:"m"`
	Year      *uint16 `json:"y"`
	Status    *uint8  `json:"status"`
	Platform  uint16  `json:"platform"`
	Region    uint8   `json:"region"`
	UpdatedAt uint32  `json:"updated_at"`
	Checksum  string  `json:"checksum"`
}

type Screenshot struct {
	ID           uint32  `json:"id"`
	AlphaChannel bool    `json:"alpha_channel"`
	Animated     bool    `json:"animated"`
	ImageID      string  `json:"image_id"`
	Width        *uint16 `json:"width"`
	Height       *uint16 `json:"height"`
	Checksum     string  `json:"checksum"`
}

type Video struct {
	ID       uint32  `json:"id"`
	Name     *string `json:"name"`
	VideoId  string  `json:"video_id"`
	Checksum string  `json:"checksum"`
}

type Website struct {
	ID       uint32 `json:"id"`
	Category uint16 `json:"category"`
	Url      string `json:"url"`
	Trusted  bool   `json:"trusted"`
	Checksum string `json:"checksum"`
}

// DB structs
type GameBase struct {
	ID                    uint32
	Name                  string
	Slug                  string
	AggregatedRating      float32 `gorm:"column:rating"`
	AggregatedRatingCount uint32  `gorm:"column:reviewsCount"`
	Category              uint8
	FirstReleaseDate      *time.Time `gorm:"column:firstReleaseDate"`
	Follows               *uint32    `gorm:"default:0"`
	Hypes                 *uint32    `gorm:"default:0"`
	Status                *uint8
	Summary               *string
	VersionTitle          *string   `gorm:"column:versionTitle"`
	UpdatedAt             time.Time `gorm:"column:updatedAt"`
	Checksum              string
	MainSeriesId          *uint32 `gorm:"column:mainSeriesId"`
	MainFranchiseId       *uint32 `gorm:"column:mainFranchiseId"`
}

type AgeRatingDB struct {
	ID             uint32
	Category       uint16
	Rating         uint16
	RatingCoverUrl *string `gorm:"column:ratingCoverUrl"`
	Synopsis       *string
	Checksum       string
	GameId         uint32 `gorm:"column:gameId"`
}

type ContentDescriptionDB struct {
	ID          uint32
	Category    uint16
	Description string
	Checksum    string
	AgeRatingId uint32 `gorm:"column:ageRatingId"`
}

type AltNameDB struct {
	ID       uint32
	Name     string
	Comment  *string
	Checksum string
	GameId   uint32 `gorm:"column:gameId"`
}

type CoverDB struct {
	ID           uint32
	AlphaChannel bool `gorm:"column:aplhaChannel"`
	Animated     bool
	ImageID      string `gorm:"column:imageId"`
	Width        *uint16
	Height       *uint16
	Checksum     string
	GameId       uint32 `gorm:"column:gameId"`
}

type LocalizationDB struct {
	ID        uint32
	Name      string
	RegionId  *uint16   `gorm:"column:regionId"`
	UpdatedAt time.Time `gorm:"column:updatedAt"`
	Checksum  string
	GameId    uint32 `gorm:"column:gameId"`
}

type ExternalServiceDB struct {
	ID         uint32
	Name       *string
	Category   uint16
	Countries  pq.Int32Array
	Media      *uint8
	PlatformId *uint16 `gorm:"column:platformId"`
	Url        *string
	UpdatedAt  time.Time `gorm:"column:updatedAt"`
	Checksum   string
	GameId     uint32 `gorm:"column:gameId"`
}

type LanguageSupportDB struct {
	ID            uint32
	LanguageId    uint8     `gorm:"column:languageId"`
	SupportTypeId uint8     `gorm:"column:supportTypeId"`
	UpdatedAt     time.Time `gorm:"column:updatedAt"`
	Checksum      string
	GameId        uint32 `gorm:"column:gameId"`
}

type ReleaseDateDB struct {
	ID         uint32
	Category   uint8
	Date       *time.Time
	Human      string
	Month      *uint8  `gorm:"column:m"`
	Year       *uint16 `gorm:"column:y"`
	StatusId   *uint8  `gorm:"column:statusId"`
	PlatformId uint16  `gorm:"column:platformId"`
	Region     uint8
	UpdatedAt  time.Time `gorm:"column:updatedAt"`
	Checksum   string
	GameId     uint32 `gorm:"column:gameId"`
}

type ScreenshotDB struct {
	ID           uint32
	AlphaChannel bool `gorm:"column:alphaChannel"`
	Animated     bool
	ImageID      string `gorm:"column:imageId"`
	Width        *uint16
	Height       *uint16
	Checksum     string
	GameId       uint32 `gorm:"column:gameId"`
}

type VideoDB struct {
	ID       uint32
	Name     *string
	VideoId  string `gorm:"column:videoId"`
	Checksum string
	GameId   uint32 `gorm:"column:gameId"`
}

type WebsiteDB struct {
	ID       uint32
	Category uint16
	Url      string
	Trusted  bool
	Checksum string
	GameId   uint32 `gorm:"column:gameId"`
}

type CollectionDB struct {
	ID        uint32
	Name      string
	Slug      string
	TypeId    uint32    `gorm:"column:typeId"`
	UpdatedAt time.Time `gorm:"column:updatedAt"`
	Checksum  string
}

type FranchiseDB struct {
	ID        uint32
	Name      string
	Slug      string
	UpdatedAt time.Time `gorm:"column:updatedAt"`
	Checksum  string
}

type EngineDB struct {
	ID          uint32
	Name        string
	Slug        string
	Description *string
	UpdatedAt   time.Time `gorm:"column:updatedAt"`
	Checksum    string
}

type GameCollection struct {
	GameId       uint32 `gorm:"column:gameId"`
	CollectionId uint32 `gorm:"column:collectionId"`
}

type GameFranchise struct {
	GameId      uint32 `gorm:"column:gameId"`
	FranchiseId uint32 `gorm:"column:franchiseId"`
}

type GameEngine struct {
	GameId   uint32 `gorm:"column:gameId"`
	EngineId uint32 `gorm:"column:engineId"`
}

type GameMode struct {
	GameId uint32 `gorm:"column:gameId"`
	ModeId uint8  `gorm:"column:modeId"`
}

type GameGenre struct {
	GameId  uint32 `gorm:"column:gameId"`
	GenreId uint8  `gorm:"column:genreId"`
}

type GamePlayerPerspective struct {
	GameId        uint32 `gorm:"column:gameId"`
	PerspectiveId uint16 `gorm:"column:perspectiveId"`
}

type GamePlatform struct {
	GameId     uint32 `gorm:"column:gameId"`
	PlatformId uint16 `gorm:"column:platformId"`
}

type GameTheme struct {
	GameId  uint32 `gorm:"column:gameId"`
	ThemeId uint16 `gorm:"column:themeId"`
}

var (
	limiter = rate.NewLimiter(rate.Every(time.Second/4), 1)
)

func Games(w http.ResponseWriter, r *http.Request) {
	updateGames()
	fmt.Fprintf(w, "Finished updating games DB")
}

func fetchData(pageNum uint8) ([]byte, error) {
	if err := limiter.Wait(context.Background()); err != nil {
		fmt.Printf("Rate limit exceeded for Page %d: %v\n", pageNum, err)
	}

	offsetValue := uint32(pageNum-1) * 500
	reqBodyString := fmt.Sprintf(`fields *, age_ratings.*, age_ratings.content_descriptions.*, alternative_names.*, cover.*, game_localizations.*, external_games.*, language_supports.*, release_dates.*, screenshots.*, videos.*, websites.*, collection.*, collections.*, franchise.*, franchises.*, game_engines.*;	where themes != (42); limit 500; offset %d; sort updated_at desc;`, offsetValue)
	reqBody := []byte(reqBodyString)

	url := "https://api.igdb.com/v4/games"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Client-ID", os.Getenv("TWITCH_CLIENT_ID"))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("TWITCH_TOKEN"))
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

func convertToDate(input *uint32) *time.Time {
	var result *time.Time

	if input != nil {
		result = &time.Time{}
		*result = time.Unix(int64(*input), 0)
	}

	return result
}

func fetchAndProcessData(pageNum uint8,
	gameBaseCh chan GameBase,
	ageRatingCh chan AgeRatingDB,
	contentDescCh chan ContentDescriptionDB,
	altNameCh chan AltNameDB,
	coverCh chan CoverDB,
	localizationCh chan LocalizationDB,
	externalServiceCh chan ExternalServiceDB,
	languageSupportCh chan LanguageSupportDB,
	releaseDateCh chan ReleaseDateDB,
	screenshotCh chan ScreenshotDB,
	videoCh chan VideoDB,
	websiteCh chan WebsiteDB,
	collectionCh chan CollectionDB,
	franchiseCh chan FranchiseDB,
	engineCh chan EngineDB,
	gameCollectionCh chan GameCollection,
	gameFranchiseCh chan GameFranchise,
	gameEngineCh chan GameEngine,
	gameModeCh chan GameMode,
	gameGenreCh chan GameGenre,
	gamePlayerPerspectiveCh chan GamePlayerPerspective,
	gamePlatformCh chan GamePlatform,
	gameThemeCh chan GameTheme,
) {
	body, err := fetchData(pageNum)
	if err != nil {
		fmt.Printf("Error fetching details for Page %d: %v\n", pageNum, err)
		return
	}
	var games []Game
	err = json.Unmarshal(body, &games)
	if err != nil {
		fmt.Println("Error parsing JSON data for Page:", pageNum, err)
		return
	}

	for _, game := range games {
		var gameBase = GameBase{
			ID:                    game.ID,
			Name:                  game.Name,
			Slug:                  game.Slug,
			AggregatedRating:      game.AggregatedRating,
			AggregatedRatingCount: game.AggregatedRatingCount,
			Category:              game.Category,
			FirstReleaseDate:      convertToDate(game.FirstReleaseDate),
			Follows:               game.Follows,
			Hypes:                 game.Hypes,
			Status:                game.Status,
			Summary:               game.Summary,
			VersionTitle:          game.VersionTitle,
			Checksum:              game.Checksum,
		}

		if game.Collection != nil {
			gameBase.MainSeriesId = &game.Collection.ID
		}
		if game.Franchise != nil {
			gameBase.MainFranchiseId = &game.Franchise.ID
		}

		gameBaseCh <- gameBase

		for _, ageRating := range game.AgeRatings {
			ageRatingCh <- AgeRatingDB{
				ID:             ageRating.ID,
				Category:       ageRating.Category,
				Rating:         ageRating.Category,
				RatingCoverUrl: ageRating.RatingCoverUrl,
				Synopsis:       ageRating.Synopsis,
				Checksum:       ageRating.Checksum,
				GameId:         game.ID,
			}

			for _, contentDesc := range ageRating.ContentDescriptions {
				contentDescCh <- ContentDescriptionDB{
					ID:          contentDesc.ID,
					Category:    contentDesc.Category,
					Description: contentDesc.Description,
					Checksum:    contentDesc.Checksum,
					AgeRatingId: ageRating.ID,
				}
			}
		}

		for _, altName := range game.AlternativeNames {
			altNameCh <- AltNameDB{
				ID:       altName.ID,
				Name:     altName.Name,
				Comment:  altName.Comment,
				Checksum: altName.Checksum,
				GameId:   game.ID,
			}
		}

		if game.Cover != nil {
			coverCh <- CoverDB{
				ID:           game.Cover.ID,
				AlphaChannel: game.Cover.AlphaChannel,
				Animated:     game.Cover.Animated,
				ImageID:      game.Cover.ImageID,
				Width:        game.Cover.Width,
				Height:       game.Cover.Height,
				Checksum:     game.Cover.Checksum,
				GameId:       game.ID,
			}
		}

		for _, localization := range game.GameLocalizations {
			localizationCh <- LocalizationDB{
				ID:       localization.ID,
				Name:     localization.Name,
				RegionId: localization.Region,
				Checksum: localization.Checksum,
				GameId:   game.ID,
			}
		}

		for _, externalService := range game.ExternalGames {
			externalServiceCh <- ExternalServiceDB{
				ID:         externalService.ID,
				Name:       externalService.Name,
				Category:   externalService.Category,
				Countries:  externalService.Countries,
				Media:      externalService.Media,
				PlatformId: externalService.Platform,
				Url:        externalService.Url,
				Checksum:   externalService.Checksum,
				GameId:     game.ID,
			}
		}

		for _, langSupp := range game.LanguageSupports {
			languageSupportCh <- LanguageSupportDB{
				ID:            langSupp.ID,
				LanguageId:    langSupp.Language,
				SupportTypeId: langSupp.LanguageSupportType,
				Checksum:      langSupp.Checksum,
				GameId:        game.ID,
			}
		}

		for _, releaseDate := range game.ReleaseDates {
			releaseDateCh <- ReleaseDateDB{
				ID:         releaseDate.ID,
				Category:   releaseDate.Category,
				Date:       convertToDate(releaseDate.Date),
				Human:      releaseDate.Human,
				Month:      releaseDate.Month,
				Year:       releaseDate.Year,
				StatusId:   releaseDate.Status,
				PlatformId: releaseDate.Platform,
				Region:     releaseDate.Region,
				Checksum:   releaseDate.Checksum,
				GameId:     game.ID,
			}
		}

		for _, screenshot := range game.Screenshots {
			screenshotCh <- ScreenshotDB{
				ID:           screenshot.ID,
				AlphaChannel: screenshot.AlphaChannel,
				Animated:     screenshot.Animated,
				ImageID:      screenshot.ImageID,
				Width:        screenshot.Width,
				Height:       screenshot.Height,
				Checksum:     screenshot.Checksum,
				GameId:       game.ID,
			}
		}

		for _, video := range game.Videos {
			videoCh <- VideoDB{
				ID:       video.ID,
				Name:     video.Name,
				VideoId:  video.VideoId,
				Checksum: video.Checksum,
				GameId:   game.ID,
			}
		}

		for _, website := range game.Websites {
			websiteCh <- WebsiteDB{
				ID:       website.ID,
				Category: website.Category,
				Url:      website.Url,
				Trusted:  website.Trusted,
				Checksum: website.Checksum,
				GameId:   game.ID,
			}
		}

		if game.Collection != nil {
			collectionCh <- CollectionDB{
				ID:       game.Collection.ID,
				Name:     game.Collection.Name,
				Slug:     game.Collection.Slug,
				TypeId:   game.Collection.TypeId,
				Checksum: game.Collection.Checksum,
			}
		}

		for _, collection := range game.Collections {
			collectionCh <- CollectionDB{
				ID:       collection.ID,
				Name:     collection.Name,
				Slug:     collection.Slug,
				TypeId:   1,
				Checksum: collection.Checksum,
			}

			gameCollectionCh <- GameCollection{
				GameId:       game.ID,
				CollectionId: collection.ID,
			}
		}

		if game.Franchise != nil {
			franchiseCh <- FranchiseDB{
				ID:       game.Franchise.ID,
				Name:     game.Franchise.Name,
				Slug:     game.Franchise.Slug,
				Checksum: game.Franchise.Checksum,
			}
		}

		for _, franchise := range game.Franchises {
			franchiseCh <- FranchiseDB{
				ID:       franchise.ID,
				Name:     franchise.Name,
				Slug:     franchise.Slug,
				Checksum: franchise.Checksum,
			}

			gameFranchiseCh <- GameFranchise{
				GameId:      game.ID,
				FranchiseId: franchise.ID,
			}
		}

		for _, engine := range game.GameEngines {
			engineCh <- EngineDB{
				ID:          engine.ID,
				Name:        engine.Name,
				Slug:        engine.Slug,
				Description: engine.Description,
				Checksum:    engine.Checksum,
			}

			gameEngineCh <- GameEngine{
				GameId:   game.ID,
				EngineId: engine.ID,
			}
		}

		for _, mode := range game.GameModes {
			gameModeCh <- GameMode{
				GameId: game.ID,
				ModeId: mode,
			}
		}

		for _, genre := range game.Genres {
			gameGenreCh <- GameGenre{
				GameId:  game.ID,
				GenreId: genre,
			}
		}

		for _, perspective := range game.PlayerPerspectives {
			gamePlayerPerspectiveCh <- GamePlayerPerspective{
				GameId:        game.ID,
				PerspectiveId: perspective,
			}
		}

		for _, platform := range game.Platforms {
			gamePlatformCh <- GamePlatform{
				GameId:     game.ID,
				PlatformId: platform,
			}
		}

		for _, theme := range game.Themes {
			gameThemeCh <- GameTheme{
				GameId:  game.ID,
				ThemeId: theme,
			}
		}
	}
}

func updateGames() {
	fmt.Printf("Started updating games at %s \n", time.Now().Format("15:04:05"))

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

	const batchSize = 3000
	const totalPages = 16

	gameBaseCh := make(chan GameBase, 100000)
	ageRatingCh := make(chan AgeRatingDB, 100000)
	contentDescCh := make(chan ContentDescriptionDB, 100000)
	altNameCh := make(chan AltNameDB, 100000)
	coverCh := make(chan CoverDB, 100000)
	localizationCh := make(chan LocalizationDB, 100000)
	externalServiceCh := make(chan ExternalServiceDB, 100000)
	languageSupportCh := make(chan LanguageSupportDB, 100000)
	releaseDateCh := make(chan ReleaseDateDB, 100000)
	screenshotCh := make(chan ScreenshotDB, 100000)
	videoCh := make(chan VideoDB, 100000)
	websiteCh := make(chan WebsiteDB, 100000)
	collectionCh := make(chan CollectionDB, 100000)
	franchiseCh := make(chan FranchiseDB, 100000)
	engineCh := make(chan EngineDB, 100000)
	gameCollectionCh := make(chan GameCollection, 100000)
	gameFranchiseCh := make(chan GameFranchise, 100000)
	gameEngineCh := make(chan GameEngine, 100000)
	gameModeCh := make(chan GameMode, 100000)
	gameGenreCh := make(chan GameGenre, 100000)
	gamePlayerPerspectiveCh := make(chan GamePlayerPerspective, 100000)
	gamePlatformCh := make(chan GamePlatform, 100000)
	gameThemeCh := make(chan GameTheme, 100000)

	go func() {
		var wgFetch sync.WaitGroup
		var i uint8
		for i = 1; i <= totalPages; i++ {
			wgFetch.Add(1)
			go func(i uint8) {
				defer wgFetch.Done()
				pageNum := i
				fetchAndProcessData(pageNum,
					gameBaseCh,
					ageRatingCh,
					contentDescCh,
					altNameCh,
					coverCh,
					localizationCh,
					externalServiceCh,
					languageSupportCh,
					releaseDateCh,
					screenshotCh,
					videoCh,
					websiteCh,
					collectionCh,
					franchiseCh,
					engineCh,
					gameCollectionCh,
					gameFranchiseCh,
					gameEngineCh,
					gameModeCh,
					gameGenreCh,
					gamePlayerPerspectiveCh,
					gamePlatformCh,
					gameThemeCh)
			}(i)
		}
		wgFetch.Wait()
		close(gameBaseCh)
		close(ageRatingCh)
		close(contentDescCh)
		close(altNameCh)
		close(coverCh)
		close(localizationCh)
		close(externalServiceCh)
		close(languageSupportCh)
		close(releaseDateCh)
		close(screenshotCh)
		close(videoCh)
		close(websiteCh)
		close(collectionCh)
		close(franchiseCh)
		close(engineCh)
		close(gameCollectionCh)
		close(gameFranchiseCh)
		close(gameEngineCh)
		close(gameModeCh)
		close(gameGenreCh)
		close(gamePlayerPerspectiveCh)
		close(gamePlatformCh)
		close(gameThemeCh)
	}()

	var wgWriteRefTables sync.WaitGroup
	wgWriteRefTables.Add(1)
	go func() {
		defer wgWriteRefTables.Done()
		writeCollectionRefRows(db, collectionCh, batchSize)
		writeFranchiseRefRows(db, franchiseCh, batchSize)
		writeEngineRefRows(db, engineCh, batchSize)
	}()

	var wgWriteBase sync.WaitGroup
	wgWriteBase.Add(1)
	go func() {
		defer wgWriteBase.Done()
		writeBaseRows(db, gameBaseCh, batchSize)
	}()
	wgWriteBase.Wait()

	var wgWriteChild sync.WaitGroup
	wgWriteChild.Add(1)
	go func() {
		defer wgWriteChild.Done()
		writeAgeRatingRows(db, ageRatingCh, batchSize)
		writeAltNameRows(db, altNameCh, batchSize)
		writeCoverRows(db, coverCh, batchSize)
		writeLocalizationRows(db, localizationCh, batchSize)
		writeExternalServiceRows(db, externalServiceCh, batchSize)
		writeLanguageSupportRows(db, languageSupportCh, batchSize)
		writeReleaseDateRows(db, releaseDateCh, batchSize)
		writeScreenshotRows(db, screenshotCh, batchSize)
		writeVideoRows(db, videoCh, batchSize)
		writeWebsiteRows(db, websiteCh, batchSize)
	}()
	wgWriteChild.Wait()

	var wgWriteJoin sync.WaitGroup
	wgWriteJoin.Add(1)
	go func() {
		defer wgWriteJoin.Done()
		writeGameCollectionRows(db, gameCollectionCh, batchSize)
		writeGameFranchiseRows(db, gameFranchiseCh, batchSize)
		writeGameEngineRows(db, gameEngineCh, batchSize)
		writeGameModeRows(db, gameModeCh, batchSize)
		writeGameGenreRows(db, gameGenreCh, batchSize)
		writeGamePlayerPerspectiveRows(db, gamePlayerPerspectiveCh, batchSize)
		writeGamePlatformRows(db, gamePlatformCh, batchSize)
		writeGameThemeRows(db, gameThemeCh, batchSize)
	}()
	wgWriteJoin.Wait()

	var wgWriteChildSecond sync.WaitGroup
	wgWriteChildSecond.Add(1)
	go func() {
		defer wgWriteChildSecond.Done()
		writeContentDescRows(db, contentDescCh, batchSize)
	}()

	fmt.Println("Successfully fetched data and written to the DB")
}

func writeBaseRows(db *gorm.DB, dataChannel chan GameBase, batchSize int) {
	var batch []GameBase
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeBasesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameBase{}
		}
	}

	if len(batch) > 0 {
		if err := writeBasesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeBasesBatch(db *gorm.DB, objects []GameBase) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("Game").Model(&GameBase{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeAgeRatingRows(db *gorm.DB, dataChannel chan AgeRatingDB, batchSize int) {
	var batch []AgeRatingDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeAgeRatingsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []AgeRatingDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeAgeRatingsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeAgeRatingsBatch(db *gorm.DB, objects []AgeRatingDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GAgeRating").Model(&AgeRatingDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeContentDescRows(db *gorm.DB, dataChannel chan ContentDescriptionDB, batchSize int) {
	var batch []ContentDescriptionDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeContentDescsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []ContentDescriptionDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeContentDescsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeContentDescsBatch(db *gorm.DB, objects []ContentDescriptionDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GAgeRatingDescription").Model(&ContentDescriptionDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeAltNameRows(db *gorm.DB, dataChannel chan AltNameDB, batchSize int) {
	var batch []AltNameDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeAltNamesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []AltNameDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeAltNamesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeAltNamesBatch(db *gorm.DB, objects []AltNameDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GAltName").Model(&AltNameDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeCoverRows(db *gorm.DB, dataChannel chan CoverDB, batchSize int) {
	var batch []CoverDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeCoversBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []CoverDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeCoversBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeCoversBatch(db *gorm.DB, objects []CoverDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GCover").Model(&CoverDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeLocalizationRows(db *gorm.DB, dataChannel chan LocalizationDB, batchSize int) {
	var batch []LocalizationDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeLocalizationsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []LocalizationDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeLocalizationsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeLocalizationsBatch(db *gorm.DB, objects []LocalizationDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("GLocalization").Model(&LocalizationDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeExternalServiceRows(db *gorm.DB, dataChannel chan ExternalServiceDB, batchSize int) {
	var batch []ExternalServiceDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeExternalServicesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []ExternalServiceDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeExternalServicesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeExternalServicesBatch(db *gorm.DB, objects []ExternalServiceDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("GExternalService").Model(&ExternalServiceDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}
func writeLanguageSupportRows(db *gorm.DB, dataChannel chan LanguageSupportDB, batchSize int) {
	var batch []LanguageSupportDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeLanguageSupportsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []LanguageSupportDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeLanguageSupportsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeLanguageSupportsBatch(db *gorm.DB, objects []LanguageSupportDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("GLanguageSupport").Model(&LanguageSupportDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeReleaseDateRows(db *gorm.DB, dataChannel chan ReleaseDateDB, batchSize int) {
	var batch []ReleaseDateDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeReleaseDatesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []ReleaseDateDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeReleaseDatesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeReleaseDatesBatch(db *gorm.DB, objects []ReleaseDateDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{UpdateAll: true}).Table("GReleaseDate").Model(&ReleaseDateDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeScreenshotRows(db *gorm.DB, dataChannel chan ScreenshotDB, batchSize int) {
	var batch []ScreenshotDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeScreenshotsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []ScreenshotDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeScreenshotsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeScreenshotsBatch(db *gorm.DB, objects []ScreenshotDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GScreenshot").Model(&ScreenshotDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeVideoRows(db *gorm.DB, dataChannel chan VideoDB, batchSize int) {
	var batch []VideoDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeVideosBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []VideoDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeVideosBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeVideosBatch(db *gorm.DB, objects []VideoDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GVideo").Model(&VideoDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeWebsiteRows(db *gorm.DB, dataChannel chan WebsiteDB, batchSize int) {
	var batch []WebsiteDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeWebsitesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []WebsiteDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeWebsitesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeWebsitesBatch(db *gorm.DB, objects []WebsiteDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GWebsite").Model(&WebsiteDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeCollectionRefRows(db *gorm.DB, dataChannel chan CollectionDB, batchSize int) {
	var batch []CollectionDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeCollectionRefsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []CollectionDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeCollectionRefsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeCollectionRefsBatch(db *gorm.DB, objects []CollectionDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GCollection").Model(&CollectionDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeFranchiseRefRows(db *gorm.DB, dataChannel chan FranchiseDB, batchSize int) {
	var batch []FranchiseDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeFranchiseRefsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []FranchiseDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeFranchiseRefsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeFranchiseRefsBatch(db *gorm.DB, objects []FranchiseDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GFranchise").Model(&FranchiseDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeEngineRefRows(db *gorm.DB, dataChannel chan EngineDB, batchSize int) {
	var batch []EngineDB
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeEngineRefsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []EngineDB{}
		}
	}

	if len(batch) > 0 {
		if err := writeEngineRefsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeEngineRefsBatch(db *gorm.DB, objects []EngineDB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GEngine").Model(&EngineDB{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

// Join tables
func writeGameCollectionRows(db *gorm.DB, dataChannel chan GameCollection, batchSize int) {
	var batch []GameCollection
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGameCollectionsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameCollection{}
		}
	}

	if len(batch) > 0 {
		if err := writeGameCollectionsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGameCollectionsBatch(db *gorm.DB, objects []GameCollection) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GameCollection").Model(&GameCollection{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGameFranchiseRows(db *gorm.DB, dataChannel chan GameFranchise, batchSize int) {
	var batch []GameFranchise
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGameFranchisesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameFranchise{}
		}
	}

	if len(batch) > 0 {
		if err := writeGameFranchisesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGameFranchisesBatch(db *gorm.DB, objects []GameFranchise) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GameFranchise").Model(&GameFranchise{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGameEngineRows(db *gorm.DB, dataChannel chan GameEngine, batchSize int) {
	var batch []GameEngine
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGameEnginesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameEngine{}
		}
	}

	if len(batch) > 0 {
		if err := writeGameEnginesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGameEnginesBatch(db *gorm.DB, objects []GameEngine) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GameEngine").Model(&GameEngine{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGameModeRows(db *gorm.DB, dataChannel chan GameMode, batchSize int) {
	var batch []GameMode
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGameModesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameMode{}
		}
	}

	if len(batch) > 0 {
		if err := writeGameModesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGameModesBatch(db *gorm.DB, objects []GameMode) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GameMode").Model(&GameMode{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGameGenreRows(db *gorm.DB, dataChannel chan GameGenre, batchSize int) {
	var batch []GameGenre
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGameGenresBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameGenre{}
		}
	}

	if len(batch) > 0 {
		if err := writeGameGenresBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGameGenresBatch(db *gorm.DB, objects []GameGenre) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GameGenre").Model(&GameGenre{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGamePlayerPerspectiveRows(db *gorm.DB, dataChannel chan GamePlayerPerspective, batchSize int) {
	var batch []GamePlayerPerspective
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGamePlayerPerspectivesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GamePlayerPerspective{}
		}
	}

	if len(batch) > 0 {
		if err := writeGamePlayerPerspectivesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGamePlayerPerspectivesBatch(db *gorm.DB, objects []GamePlayerPerspective) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GamePlayerPerspective").Model(&GamePlayerPerspective{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGamePlatformRows(db *gorm.DB, dataChannel chan GamePlatform, batchSize int) {
	var batch []GamePlatform
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGamePlatformsBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GamePlatform{}
		}
	}

	if len(batch) > 0 {
		if err := writeGamePlatformsBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGamePlatformsBatch(db *gorm.DB, objects []GamePlatform) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GamePlatform").Model(&GamePlatform{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}

func writeGameThemeRows(db *gorm.DB, dataChannel chan GameTheme, batchSize int) {
	var batch []GameTheme
	for entry := range dataChannel {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			if err := writeGameThemesBatch(db, batch); err != nil {
				fmt.Println("Error writing batch:", err)
			}
			batch = []GameTheme{}
		}
	}

	if len(batch) > 0 {
		if err := writeGameThemesBatch(db, batch); err != nil {
			fmt.Println("Error writing final batch:", err)
		}
	}
}
func writeGameThemesBatch(db *gorm.DB, objects []GameTheme) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.WithContext(context.Background()).Clauses(clause.OnConflict{DoNothing: true}).Table("GameTheme").Model(&GameTheme{}).Create(&objects).Error; err != nil {
			return err
		}
		return nil
	})
}
