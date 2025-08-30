package moshtix

import (
	"context"
	"fmt"
	"github.com/dbschema"
	"github.com/google/uuid"
	"github.com/hasura/go-graphql-client"
	"github.com/pipeline"
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

func convertToDbEvent(item moshtixItem) dbschema.Event {
	//tags := make([]string, 0, len(item.Tags.Items))
	/*for _, tag := range item.Tags.Items {
		tags = append(tags, tag.Name)
	}*/

	var result = dbschema.Event{EventID: uuid.NewString(),
		Source_name: string(dbschema.Moshtix),
		SourceEvent: strconv.Itoa(item.Id),
		Title:       item.Name,
		Description: item.Description,
		Start:       item.StartDate,
		End:         item.EndDate,
		VenueName:   item.Venue.Name,
		URL:         item.EventUrl,
		FetchedAt:   time.Now(),
	}

	if len(item.TicketTypes.Items) > 0 {
		result.PriceMin = item.TicketTypes.Items[0].TicketPrice
		result.PriceMax = item.TicketTypes.Items[len(item.TicketTypes.Items)-1].TicketPrice
	}

	for _, tag := range item.Tags.Items {
		result.Tags = append(result.Tags, tag.Name)
	}

	for _, imageUrl := range item.Images.Items {
		result.Images = append(result.Images, imageUrl.Url)
	}

	result.ContentFlags.EighteenPlus = (item.AgeRestriction == "OVER18")

	if item.Venue.Address != nil {
		result.Address = dbschema.Address{
			Line1:    item.Venue.Address.Line1,
			Line2:    item.Venue.Address.Line2,
			PostCode: item.Venue.Address.PostCode,
			Locality: item.Venue.Address.Locality,
			Region:   item.Venue.Address.Region,
			Country:  item.Venue.Address.Country,
		}
	}

	if item.Venue.Location != nil {
		result.Geo = dbschema.Geo{
			Lat: item.Venue.Location.Latitude,
			Lng: item.Venue.Location.Longitude,
		}
	}

	return result
}

func Scrape(pipeline pipeline.Pipeline) error {
	var moshtixResponse = moshtixResponse{}

	var pageIndex = 0

	for {
		fmt.Println("Getting page", pageIndex)

		// Variables
		vars := map[string]any{
			"pageIndex":          graphql.Int(pageIndex),
			"pageSize":           IntBetween1and200(100),
			"sortBy":             EventSortOptionsInput("STARTDATE"),
			"sortByDirection":    SortByDirectionInput("ASC"),
			"eventStartDateFrom": Date("2025-08-25T00:00:00.000+10:00"),
			"location":           EventLocationInput{Latitude: -33.8727, Longitude: 151.2057, WithinRadius: 10000},
			"region":             []RegionInput{RegionInput("NSW")},
		}

		client := graphql.NewClient("https://api.moshtix.com/v1/graphql", nil).WithDebug(true)
		err := client.Query(context.Background(), &moshtixResponse, vars)
		if err != nil {
			fmt.Printf(err.Error())
			// Handle error.
		}

		for _, element := range moshtixResponse.Viewer.GetEvents.Items {
			// element is the element from someSlice for where we are

			dbEvent := convertToDbEvent(element)
			_, err := pipeline.Process(dbEvent)
			if err != nil {
				fmt.Println(err.Error())
			}
		}

		if moshtixResponse.Viewer.GetEvents.PageInfo.HasNextPage {
			pageIndex++
		} else {
			break
		}

		time.Sleep(3 * time.Second)
	}

	fmt.Println(moshtixResponse.Viewer.GetEvents.TotalCount)
	return nil
}
