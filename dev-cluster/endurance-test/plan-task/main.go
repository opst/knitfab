package main

import (
	"flag"
	"os"
	"time"
)

func main() {
	in := flag.String("in", "/in/file", "input file")
	slp := flag.Int("sleep", 0, "sleep time in seconds")
	out := flag.String("out", "/out/file", "output file")
	flag.Parse()

	// Sleep for the specified duration
	if *slp > 0 {
		time.Sleep(time.Duration(*slp) * time.Second)
	}

	// copy in/file to out/file only first 1 line, and append current timestamp to the end
	fin, err := os.Open(*in)
	if err != nil {
		panic(err)
	}
	defer fin.Close()

	fout, err := os.Create(*out)
	if err != nil {
		panic(err)
	}
	defer fout.Close()

	// Read the input file and write the last 10 lines to the output file
	line := ""
	for {
		buf := make([]byte, 1)
		n, err := fin.Read(buf)
		if err != nil {
			break
		}
		if buf[0] == '\n' {
			break
		} else {
			line += string(buf[:n])
		}
	}

	if line != "" {
		// write the line to the output file
		if _, err := fout.WriteString(line); err != nil {
			panic(err)
		}
		if _, err := fout.WriteString("\n"); err != nil {
			panic(err)
		}
	}
	// write the current timestamp to the output file
	if _, err := fout.WriteString(time.Now().Format(time.RFC3339)); err != nil {
		panic(err)
	}
	// write the line to the output file
	if _, err := fout.WriteString("\n"); err != nil {
		panic(err)
	}
}
