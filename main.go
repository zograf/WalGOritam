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

func main() {
	src.TestCache()
	// Engine initialization
	engine := src.EngineInit()
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.ReplaceAll(text, "\n", "")
		if text == "EXIT" {
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
				engine.EngineGet(tokens[1])
				continue
			}
		} else if tokens[0] == "DEL" {
			if len(tokens) == 2 {
				engine.EngineDelete(tokens[1])
				continue
			}
		}
		help()
	}
}
