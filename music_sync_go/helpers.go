package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"log/slog"

	"go.senan.xyz/taglib"
)

type songTagData struct {
	SourceFile string
	Artist     string
	Title      string
	Album      string
}

func checkDeps() {
	if !hasTool("ffmpeg") || !hasTool("ffprobe") {
		slog.Error("Missing required tools: ffmpeg/ffprobe")
		slog.Info("Please install: sudo dnf install ffmpeg")
		os.Exit(1)
	}
}

func copyFileSimple(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	_, err = io.Copy(out, in)
	return err
}

func getSongMetadataForFiles(ctx context.Context, files []string, alreadyTagged map[string]songTagData, stopper songTagData) (map[string]songTagData, error) {
	for _, file := range files {
		lFile := strings.ToLower(file)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if _, ok := alreadyTagged[lFile]; ok {
			continue
		}
		data, err := getSongMetadata(file)
		if err != nil {
			slog.Error("Error reading metadata", "file", file, "error", err)
			continue
		}
		alreadyTagged[lFile] = data
		if strings.EqualFold(stopper.SourceFile, file) || (strings.EqualFold(data.Artist, stopper.Artist) && strings.EqualFold(data.Title, stopper.Title)) {
			break
		}
	}
	return alreadyTagged, nil
}

func getSongMetadata(filename string) (songTagData, error) {
	// 1. Ensure the file actually exists
	if _, err := os.Stat(filename); err != nil {
		return songTagData{}, err
	}

	// 2. Read the tags using the proper package function
	tags, err := taglib.ReadTags(filename)
	if err != nil {
		return songTagData{}, err
	}

	// Helper function to safely get the first value of a multi-valued tag
	getFirstValue := func(tagKey string) string {
		if values, exists := tags[tagKey]; exists && len(values) > 0 {
			return values[0]
		}
		return ""
	}

	// 3. Map the retrieved metadata into your custom struct
	data := songTagData{
		SourceFile: filename,
		Artist:     getFirstValue(taglib.Artist),
		Title:      getFirstValue(taglib.Title),
		Album:      getFirstValue(taglib.Album),
	}
	if data.Artist == "" && data.Title == "" {
		// try to guess by filename if no metadata found
		base := filepath.Base(filename)
		ext := filepath.Ext(base)
		name := strings.TrimSuffix(base, ext)
		parts := strings.Split(name, " - ")
		if len(parts) >= 2 {
			data.Artist = parts[0]
			data.Title = parts[1]
		} else {
			data.Title = name
		}
	}
	return data, nil
}

func findSongMatch(source songTagData, targetMetaData map[string]songTagData) *songTagData {
	var match *songTagData = nil

	for _, targetData := range targetMetaData {
		if strings.EqualFold(filepath.Base(source.SourceFile), filepath.Base(targetData.SourceFile)) {
			match = &targetData
			break
		}
		if source.Artist != "" && source.Title != "" && strings.EqualFold(targetData.Artist, source.Artist) && strings.EqualFold(targetData.Title, source.Title) {
			match = &targetData
			break
		}
	}
	if match == nil {
		for _, targetData := range targetMetaData {
			if source.Title != "" && strings.EqualFold(targetData.Title, source.Title) {
				match = &targetData
				break
			}
		}
	}
	return match
}
