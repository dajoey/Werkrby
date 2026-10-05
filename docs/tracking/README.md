# External branch tracking pins

- `rcanavides-router.sha` — tip of `rcanavides/spynel` `feature/multi-harness-router`
  (provider-profile / HarnessRouting work). Watched by
  `.github/workflows/watch-rcanavides-router.yml`. Update the pin after
  reviewing movement; do not port that branch into SpyJo until upstream
  direction is clear.
- `upstream-reviewed.sha` — last `agent0ai/spynel` `main` tip whose unmerged commits were
  reviewed. `.github/workflows/fork-drift-alarm.yml` goes red when upstream moves past it
  (or when the file is missing); advance it after reviewing the delta, or merge upstream.
