package plugin

type BQUpstreamGenerator struct {
	parserFunc    ParserFunc
	extractorFunc ExtractorFunc
	evaluate      EvalAssetFunc
}

func (g BQUpstreamGenerator) GenerateUpstreams(assets Assets) []ResourceURN {
	// TODO: recursively generate upstream resourceURN similar with plugin_service.go.GenerateDependencies()
	_ = g.evaluate(assets) // get raw query

	return []ResourceURN{}
}
