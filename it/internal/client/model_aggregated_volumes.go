package ledgerclient

type AggregatedVolumes map[string]map[string]*Volume

func (a *AggregatedVolumes) SetVolumes(account, asset string, volumes *Volume) *AggregatedVolumes {
	if assetsVolumes, ok := (*a)[account]; !ok {
		(*a)[account] = map[string]*Volume{
			asset: volumes,
		}
	} else {
		assetsVolumes[asset] = volumes
	}
	return a
}

func NewAggregatedVolumes() *AggregatedVolumes {
	return &AggregatedVolumes{}
}
