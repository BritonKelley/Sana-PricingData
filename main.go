package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"
)

const url = "https://api.upcitemdb.com/prod/trial/lookup?upc="

var approvedDomains = []string{"walgreens.com", "cvs.com", "public.com", "walmart.com", "target.com"}

type Response struct {
	Items   []Item
	Code    string
	Message string
}

type Item struct {
	Offers               []Offer
	LowestRecordedPrice  float32 `json:"lowest_recorded_price"`
	HighestRecordedPrice float32 `json:"highest_recorded_price"`
	Upc                  string
}

type Offer struct {
	Merchant  string
	Domain    string
	ListPrice string
	Price     float32
	Link      string
}

type ItemPriceMap struct {
	Upc   string
	price float32
}

func main() {
	var upcList []string

	f, err := os.Open("upcList.txt")
	if err != nil {
		fmt.Printf("Error Opening %s", err)
	}
	defer f.Close()

	fPrice, err := os.OpenFile("PriceList.sql", os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error Opening %s", err)
	}
	defer fPrice.Close()

	fErrUpc, err := os.OpenFile("ErroredUPC.txt", os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error Opening %s", err)
	}
	defer fErrUpc.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		upc := scanner.Text()
		upcList = append(upcList, upc)
	}

	var wg sync.WaitGroup
	c := make(chan ItemPriceMap, len(upcList))
	for i := 0; i < len(upcList); i++ {
		wg.Add(1)
		go GetPrice(&wg, upcList[i], c, fErrUpc)
		time.Sleep(10 * time.Second)
	}

	go AllDone(&wg, c)

	for value := range c {
		fmt.Printf("Writing price %.2f update for %s\n", value.price, value.Upc)
		sql := fmt.Sprintf("UPDATE `sana_db`.`item` SET `price` = '%.2f' WHERE (`UPC` = '%s');\n", value.price, value.Upc)
		_, err := fPrice.WriteString(sql)
		if err != nil {
			fmt.Printf("Error writing record for %s\n", value.Upc)
		}
	}
}

func GetPrice(wg *sync.WaitGroup, upc string, c chan ItemPriceMap, errorFile *os.File) {
	defer wg.Done()

	client := http.Client{Timeout: time.Duration(30) * time.Second}

	fmt.Printf("Initial Call for: %s\n", upc)
	httpResponse, err := client.Get(url + upc)

	if err != nil {
		fmt.Printf("Unable to retrieve data for UPC: %s with error: %s\n", upc, err)
		errorFile.WriteString(upc + "\n")
		return
	}

	var count = 1
	for httpResponse.StatusCode == 429 && count < 2 {
		time.Sleep(time.Duration(count+10) * time.Second)
		count++
		fmt.Printf("Retry Call for: %s\n", upc)
		httpResponse, _ = client.Get(url + upc)
	}

	defer httpResponse.Body.Close()
	respBody, err := io.ReadAll(httpResponse.Body)
	if err != nil {
		fmt.Printf("Problem reading the body: %s\n", err)
	}

	var response Response

	json.Unmarshal(respBody, &response)

	if response.Code == "OK" && len(response.Items) > 0 {
		item := response.Items[0]
		var price float32 = 0.0

		if len(item.Offers) > 0 {
			for i := 0; i < len(item.Offers); i++ {
				if slices.Contains(approvedDomains, item.Offers[i].Domain) {
					price = item.Offers[i].Price
					break
				}
			}

		}

		if price <= 0.0 {
			price = item.HighestRecordedPrice
		}

		var result = ItemPriceMap{
			Upc:   upc,
			price: price,
		}
		c <- result
	} else {
		fmt.Printf("Unable to retrieve data for UPC: %s with code: %s\n", upc, response.Code)
		record := fmt.Sprintf("%s - %s -> %s\n", upc, response.Code, response.Message)
		errorFile.WriteString(record)
	}

}

func AllDone(wg *sync.WaitGroup, c chan ItemPriceMap) {
	wg.Wait()
	close(c)
}
