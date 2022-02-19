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
		key := tokens[1]
		value := tokens[2]
		engine.EnginePut(key, value)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	engine.ForceFlush()
}

func main() {
	//src.ReadIndex("L-2-1645283639390404Index.bin")
	src.NewConf()
	fileFlag := true
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
			if tokens[0] == "PUT" {
				if len(tokens) == 3 {
					engine.EnginePut(tokens[1], tokens[2])
					continue
				}
			} else if tokens[0] == "GET" {
				if len(tokens) == 2 {
					val, found := engine.EngineGet(tokens[1])
					fmt.Println(val, found)
					continue
				}
			} else if tokens[0] == "DEL" {
				if len(tokens) == 2 {
					deleted := engine.EngineDelete(tokens[1])
					fmt.Println(deleted)
					continue
				}
			}
			help()
		}
	}
}
