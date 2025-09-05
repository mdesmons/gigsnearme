package backend

import (
	"crypto/tls"
	"errors"
	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"net/http"
	"time"
)

type MetroScraper struct {
	logger zerolog.Logger
}

func NewMetroScraper(logger zerolog.Logger) MetroScraper {
	return MetroScraper{
		logger: logger,
	}
}

func (d MetroScraper) scrapeEvent(url string) (*Event, error) {
	d.logger.Debug().Msgf("Scraping event at %s", url)
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

// Scrape fetches the Metro Theatre upcoming events page and extracts event links
func (d MetroScraper) Scrape(pipeline Pipeline) error {
	d.logger.Debug().Msg("Starting Metro Theatre scrape")
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
		d.logger.Debug().Msg("Found evt-card section")
		if s.Is("a") {

			href, exists := s.Attr("href")
			if exists {
				d.logger.Debug().Msgf("Found link: %s\n", href)
				links = append(links, href)

			} else {
				d.logger.Debug().Msg("No link found in evt-card")
			}
		}
	})

	d.logger.Debug().Msgf("Found %d event links\n", len(links))

	for _, link := range links {
		time.Sleep(1 * time.Second) // Be polite and avoid overwhelming the server
		event, err := d.scrapeEvent(link)
		if err != nil {
			d.logger.Error().Msgf("Error scraping event at %s: %s\n", link, err.Error())
			continue
		}
		_, err = pipeline.Process(*event)
		if err != nil {
			d.logger.Error().Msgf("Error saving event %s - %s: %s\n", event.Source_name, event.SourceEvent, err.Error())
		} else {
			d.logger.Debug().Msgf("Saved event %s - %s\n", event.Source_name, event.SourceEvent)
		}
	}
	return nil
}
