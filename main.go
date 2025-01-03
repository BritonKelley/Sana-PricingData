package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const url = "https://api.barcodelookup.com/v2/products?key=xxxxxxxxxxxxxxxxxxxx&barcode="

var approvedDomains = []string{"walgreens", "cvs", "publix", "walmart", "target"}

type Response struct {
	Products []Item
}

type Item struct {
	Stores []Stores
	Upc    string `json:"barcode_number"`
}

type Stores struct {
	StoreName  string `json:"store_name"`
	StorePrice string `json:"store_price"`
}

type ItemPriceMap struct {
	Upc   string
	price float64
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
	c := make(chan ItemPriceMap)
	for i := 0; i < len(upcList); i++ {
		wg.Add(1)
		go GetPrice(&wg, upcList[i], c, fErrUpc, i)
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

func GetPrice(wg *sync.WaitGroup, upc string, c chan ItemPriceMap, errorFile *os.File, routineNumber int) {
	defer wg.Done()

	time.Sleep(time.Duration(routineNumber*2) * time.Second)
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

	if httpResponse.StatusCode == 200 && len(response.Products) > 0 {
		item := response.Products[0]
		var price float64 = 0.0

		if len(item.Stores) > 0 {
			for i := 0; i < len(item.Stores); i++ {
				if slices.Contains(approvedDomains, strings.ToLower(item.Stores[i].StoreName)) {
					parsedPrice, _ := strconv.ParseFloat(item.Stores[i].StorePrice, 64)
					price = parsedPrice
					break
				}
			}

		}

		var result = ItemPriceMap{
			Upc:   upc,
			price: price,
		}
		c <- result
	} else {
		fmt.Printf("Unable to retrieve data for UPC: %s with code: %d\n", upc, httpResponse.StatusCode)
		record := fmt.Sprintf("%s - %d -> %s\n", upc, httpResponse.StatusCode, httpResponse.Status)
		errorFile.WriteString(record)
	}

}

func AllDone(wg *sync.WaitGroup, c chan ItemPriceMap) {
	wg.Wait()
	close(c)
}
