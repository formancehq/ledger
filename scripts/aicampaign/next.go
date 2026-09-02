package main

func buildNextResult(campaign *Campaign) *NextResult {
	if campaign.Observations == nil {
		campaign.Observations = staleObservations(campaign, previousObservations(campaign), "release/v3.0")
	}
	inspection := buildInspection(campaign)
	result := &NextResult{
		SchemaVersion:      nextSchemaVersion,
		CampaignID:         campaign.CampaignID,
		AuditID:            campaign.AuditID,
		AuditedSHA:         campaign.AuditedSHA,
		BasedOnRefreshedAt: inspection.RefreshedAt,
		Freshness:          inspection.Freshness,
		Findings:           []NextFinding{},
	}
	for _, finding := range inspection.Findings {
		result.Findings = append(result.Findings, NextFinding{
			ID:            finding.ID,
			Qualification: finding.Qualification,
			State:         finding.State,
			NextAction:    finding.NextAction,
			Blockers:      finding.Blockers,
			Freshness:     finding.Freshness,
		})
	}

	return result
}
