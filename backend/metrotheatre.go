package backend

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"net/http"
	"time"
)

type MetroScraper struct {
}

func scrapeEvent(url string) (*Event, error) {
	fmt.Printf("Scraping event at %s\n", url)
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{},
		},
	}

	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}

	sel := doc.Find("h1.title").First()
	if sel.Length() == 0 {
		return nil, errors.New("no title found for event at " + url)
	}
	name := sel.Text()

	sel = doc.Find("div.post-content").First()
	if sel.Length() == 0 {
		return nil, errors.New("no description found for event at " + url)
	}
	description := sel.Text()

	sel = doc.Find("li.session-date").First()
	if sel.Length() == 0 {
		return nil, errors.New("no date found for event at " + url)
	}
	temp := sel.Text()
	startDate, err := time.Parse("Monday, 2 January 2006 03:04 PM", temp)
	if err != nil {
		startDate = time.Time{}
	}

	var result = Event{EventID: uuid.NewString(),
		Source_name: string(MetroTheatre),
		SourceEvent: url,
		Title:       name,            //h1 title
		Description: description,     //<div class='post-content'>
		Start:       startDate.UTC(), //<li class='session-date'>Friday, 31 October 2025 08:00 PM
		End:         startDate.UTC(), // <li class='session-date'>Friday, 31 October 2025 08:00 PM
		VenueName:   "Metro Theatre", // item.Venue.Name,
		URL:         url,             // event URL
		FetchedAt:   time.Now(),
		Geo: Geo{
			Lat: -33.87557496143779,
			Lng: 151.206671962522,
		},
		Address: Address{
			Line1:    "624 George St",
			PostCode: "2000",
			Locality: "Sydney",
			Region:   "NSW",
			Country:  "Australia",
		},
		Tagged: false,
	}

	return &result, nil
}

/*
func convertToDbEvent(item moshtixItem) Event {
	var result = Event{EventID: uuid.NewString(),
		Source_name: string(Moshtix),
		SourceEvent: strconv.Itoa(item.Id),
		Title:       item.Name,
		Description: item.Description,
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

*/
// Scrape fetches the Metro Theatre upcoming events page and extracts event links
func (d MetroScraper) Scrape(pipeline Pipeline) error {
	fmt.Println("Starting Metro Theatre scrape")
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{},
		},
	}

	resp, err := client.Get("https://www.metrotheatre.com.au/?s&key=upcoming")
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return err
	}

	var links []string
	doc.Find(".evt-card").Each(func(i int, s *goquery.Selection) {
		fmt.Printf("Found evt-card section\n")
		if s.Is("a") {

			href, exists := s.Attr("href")
			if exists {
				fmt.Printf("Found link: %s\n", href)
				links = append(links, href)

			} else {
				fmt.Printf("No link found in evt-card\n")
			}
		}
	})

	fmt.Printf("Found %d event links\n", len(links))

	for _, link := range links {
		time.Sleep(1 * time.Second) // Be polite and avoid overwhelming the server
		event, err := scrapeEvent(link)
		if err != nil {
			fmt.Printf("Error scraping event at %s: %s\n", link, err.Error())
			continue
		}
		_, err = pipeline.Process(*event)
		if err != nil {
			fmt.Printf("Error saving event %s - %s: %s\n", event.Source_name, event.SourceEvent, err.Error())
		} else {
			fmt.Printf("Saved event %s - %s\n", event.Source_name, event.SourceEvent)
		}
	}
	return nil
}
