package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	src "github.com/zograf/WalGOritam/src"
)

func help() {
	fmt.Println("Usage: PUT <key> <value>")
	fmt.Println("       GET <key>")
	fmt.Println("       DEL <key>")
	fmt.Println("       PUT_HLL <key> <p>")
	fmt.Println("       PUT_CMS <key> <epsilon> <delta>")
	fmt.Println("       GET_TOTAL_KEYS")
	fmt.Println("       GET_REQ_PER_KEY <key>")
	fmt.Println("")
	fmt.Println("Quit:  EXIT")
}

func fileTest(path string, engine *src.Engine) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := strings.ReplaceAll(scanner.Text(), "\n", "")
		tokens := strings.Split(text, " ")
		err, value := engine.ProcessRequest(tokens)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(value)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	engine.ForceFlush()
}

func main() {
	//src.ReadIndex("L-2-1645283639390404Index.bin")
	src.NewConf()
	fileFlag := false
	//src.TestCache()
	// Engine initialization
	engine := src.EngineInit()
	if fileFlag {
		fileTest("tests/datoteka.txt", engine)
	} else {
		for {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("> ")
			text, _ := reader.ReadString('\n')
			text = strings.ReplaceAll(text, "\n", "")
			if text == "EXIT" {
				engine.ForceFlush()
				os.Exit(0)
			}
			tokens := strings.Split(text, " ")
			if tokens[0] != "GET" && tokens[0] != "PUT" && tokens[0] != "DEL" {
				help()
				continue
			}
			err, value := engine.ProcessRequest(tokens)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(value)
		}
	}
}
