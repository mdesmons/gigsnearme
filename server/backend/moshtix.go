package backend

import (
	"context"
	"github.com/google/uuid"
	"github.com/hasura/go-graphql-client"
	"github.com/rs/zerolog"
	"jaytaylor.com/html2text"
	"strconv"
	"time"
)

// defining GraphQL customer types
type IntBetween1and200 int
type Date string
type AgeRestriction string
type EventSortOptionsInput string
type SortByDirectionInput string
type RegionInput string

type EventLocationInput struct {
	Latitude     float64 `json:"latitude"`
	Longitude    float64 `json:"longitude"`
	WithinRadius int     `json:"withinRadius"`
}

func (s IntBetween1and200) GetGraphQLType() string { return "IntBetween1and200" }

type moshtixItem struct {
	Id             int
	Name           string
	Description    string
	EventUrl       string
	AgeRestriction AgeRestriction
	StartDate      time.Time
	EndDate        time.Time
	Images         *struct {
		Items []struct {
			Url string
		}
	}
	Distance *struct {
		FromLatitude  float64
		FromLongitude float64
	}
	Genre *struct {
		Name string
	}
	Venue *struct {
		Name    string
		Address *struct {
			Line1    string
			Line2    string
			PostCode string
			Locality string
			Region   string
			Country  string
		}
		Location *struct {
			Latitude  float64
			Longitude float64
		}
	}
	Tags *struct {
		Items []struct {
			Name string
		}
	}
	TicketTypes *struct {
		Items []struct {
			Name        string
			TicketPrice float64
		}
	}
}

type moshtixResponse struct {
	Viewer struct {
		GetEvents struct {
			TotalCount int
			PageInfo   struct {
				HasPreviousPage bool
				HasNextPage     bool
				PageIndex       int
				PageSize        int
			}
			Items []moshtixItem
		} `graphql:"getEvents(location: $location, region: $region, pageIndex: $pageIndex, pageSize: $pageSize, sortBy: $sortBy, sortByDirection: $sortByDirection, eventStartDateFrom: $eventStartDateFrom)"`
	}
}

type MoshtixScraper struct {
	logger zerolog.Logger
}

func NewMoshtixScraper(logger zerolog.Logger) MoshtixScraper {
	return MoshtixScraper{
		logger: logger,
	}
}

func convertToDbEvent(item moshtixItem) Event {

	description, _ := html2text.FromString(item.Description, html2text.Options{TextOnly: true})

	var result = Event{EventID: uuid.NewString(),
		Source_name: string(Moshtix),
		SourceEvent: strconv.Itoa(item.Id),
		Title:       item.Name,
		Description: description,
		Start:       item.StartDate.UTC(),
		End:         item.EndDate.UTC(),
		VenueName:   item.Venue.Name,
		URL:         item.EventUrl,
		FetchedAt:   time.Now(),
	}

	if len(item.TicketTypes.Items) > 0 {
		result.PriceMin = item.TicketTypes.Items[0].TicketPrice
		result.PriceMax = item.TicketTypes.Items[len(item.TicketTypes.Items)-1].TicketPrice
	}

	for _, imageUrl := range item.Images.Items {
		result.Images = append(result.Images, imageUrl.Url)
	}

	result.ContentFlags.EighteenPlus = (item.AgeRestriction == "OVER18")

	if item.Venue.Address != nil {
		result.Address = Address{
			Line1:    item.Venue.Address.Line1,
			Line2:    item.Venue.Address.Line2,
			PostCode: item.Venue.Address.PostCode,
			Locality: item.Venue.Address.Locality,
			Region:   item.Venue.Address.Region,
			Country:  item.Venue.Address.Country,
		}
	}

	if item.Venue.Location != nil {
		result.Geo = Geo{
			Lat: item.Venue.Location.Latitude,
			Lng: item.Venue.Location.Longitude,
		}
	}

	return result
}

func (d MoshtixScraper) Scrape(pipeline Pipeline) error {

	var pageIndex = 0
	var pageSize = 100
	var startFrom = time.Now().Format(time.RFC3339)
	var eventsFetched = 0
	var eventsProcessed = 0

	for {
		var moshtixResponse = moshtixResponse{}
		d.logger.Debug().Msgf("Getting page %d", pageIndex)

		// Variables
		vars := map[string]any{
			"pageIndex":          graphql.Int(pageIndex),
			"pageSize":           IntBetween1and200(pageSize),
			"sortBy":             EventSortOptionsInput("STARTDATE"),
			"sortByDirection":    SortByDirectionInput("ASC"),
			"eventStartDateFrom": Date(startFrom),
			"location":           EventLocationInput{Latitude: -33.8727, Longitude: 151.2057, WithinRadius: 10000},
			"region":             []RegionInput{RegionInput("NSW")},
		}

		client := graphql.NewClient("https://api.moshtix.com/v1/graphql", nil).WithDebug(true)
		err := client.Query(context.Background(), &moshtixResponse, vars)
		if err != nil {
			d.logger.Error().Msg(err.Error())
			return err
		}

		eventsFetched += len(moshtixResponse.Viewer.GetEvents.Items)

		for _, element := range moshtixResponse.Viewer.GetEvents.Items {
			// element is the element from someSlice for where we are

			dbEvent := convertToDbEvent(element)
			_, err := pipeline.Process(dbEvent)
			if err != nil {
				d.logger.Error().Msg(err.Error())
			} else {
				eventsProcessed++
			}
		}

		if moshtixResponse.Viewer.GetEvents.PageInfo.HasNextPage {
			pageIndex++
		} else {
			break
		}

		//	time.Sleep(3 * time.Second)
	}

	d.logger.Info().Msgf("Fetched %d events, successfully processed %d events", eventsFetched, eventsProcessed)
	return nil
}
