package venuescrapers

import (
	"common"
	"crypto/tls"
	"errors"
	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"net/http"
	"time"
)

type OurSecretSpotScraper struct {
	logger zerolog.Logger
}

func NewOurSecretSpotScraper(logger zerolog.Logger) OurSecretSpotScraper {
	return OurSecretSpotScraper{
		logger: logger,
	}
}

func (obj OurSecretSpotScraper) scrapeEvent(url string) (*common.Event, error) {
	obj.logger.Debug().Msgf("Scraping event at %s", url)
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

	descriptionPanel := doc.Find("#tab-description").First()
	if sel.Length() == 0 {
		return nil, errors.New("no description found for event at " + url)
	}

	descriptionTextLines := descriptionPanel.Find("p")
	description := descriptionTextLines.Text()

	sel = doc.Find("li.session-date").First()
	if sel.Length() == 0 {
		return nil, errors.New("no date found for event at " + url)
	}
	temp := sel.Text()
	startDate, err := time.Parse("Monday, 2 January 2006 03:04 PM", temp)
	if err != nil {
		startDate = time.Time{}
	}

	var result = common.Event{EventID: uuid.NewString(),
		Source_name: string(common.OurSecretSpot),
		SourceEvent: url,
		Title:       name,              //h1 title
		Description: description,       //<div class='post-content'>
		Start:       startDate.UTC(),   //<li class='session-date'>Friday, 31 October 2025 08:00 PM
		End:         startDate.UTC(),   // <li class='session-date'>Friday, 31 October 2025 08:00 PM
		VenueName:   "Our Secret Spot", // item.Venue.Name,
		URL:         url,               // event URL
		FetchedAt:   time.Now(),
		Geo: common.Geo{
			Lat: -33.87557496143779,
			Lng: 151.206671962522,
		},
		Address: common.Address{
			Line1:    "624 George St",
			PostCode: "2000",
			Locality: "Sydney",
			Region:   "NSW",
			Country:  "Australia",
		},
		Tagged:       false,
		ContentFlags: common.ContentFlags{EighteenPlus: true, SexPositive: true},
	}

	return &result, nil
}

// Scrape fetches the OurSecretSpot Theatre upcoming events page and extracts event links
func (obj OurSecretSpotScraper) Scrape(pipeline Pipeline) error {
	obj.logger.Debug().Msg("Starting OurSecretSpot Theatre scrape")
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{},
		},
	}

	resp, err := client.Get("https://oursecretspot.com.au/events-annandale/")
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return err
	}

	var links []string
	doc.Find(".bb-link").Each(func(i int, s *goquery.Selection) {
		obj.logger.Debug().Msg("Found bb-link section")
		if s.Is("a") {

			href, exists := s.Attr("href")
			if exists {
				obj.logger.Debug().Msgf("Found link: %s\n", href)
				links = append(links, href)

			} else {
				obj.logger.Debug().Msg("No link found in bb-link")
			}
		}
	})

	obj.logger.Debug().Msgf("Found %obj event links\n", len(links))

	for _, link := range links {
		eventExists, err := pipeline.EventExists(string(common.OurSecretSpot), link)
		if err != nil {
			obj.logger.Error().Msgf("Error checking if event exists %s: %s\n", link, err.Error())
			continue
		}
		if eventExists {
			obj.logger.Debug().Msgf("Event already exists, skipping: %s\n", link)
			continue
		}

		time.Sleep(1 * time.Second) // Be polite and avoid overwhelming the server
		event, err := obj.scrapeEvent(link)
		if err != nil {
			obj.logger.Error().Msgf("Error scraping event at %s: %s\n", link, err.Error())
			continue
		}
		_, err = pipeline.Process(*event)
		if err != nil {
			obj.logger.Error().Msgf("Error saving event %s - %s: %s\n", event.Source_name, event.SourceEvent, err.Error())
		} else {
			obj.logger.Debug().Msgf("Saved event %s - %s\n", event.Source_name, event.SourceEvent)
		}
	}
	return nil
}
