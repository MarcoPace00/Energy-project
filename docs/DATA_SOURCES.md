# Data sources and licences

This project publishes only data whose licence allows it. Everything else is used to
build models, and only results derived from it (forecast errors, simulated revenues,
risk figures) are published. No raw third-party data is committed to this repository:
the pipeline downloads it.

The rule is enforced in code: each export in [energy_lab/export.py](../energy_lab/export.py)
declares `public=True` or `public=False`, and only public exports reach the website.

## Status per dataset

| Dataset | Source | ENTSO-E code | Data provider | Licence | Published? |
|---|---|---|---|---|---|
| Hourly weather (temperature, wind at 100 m, solar radiation, cloud cover) | [Open-Meteo](https://open-meteo.com/) historical weather API | n/a | Open-Meteo | CC BY 4.0 | ✅ yes, aggregated |
| Day-ahead total load forecast | [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) | 6.1.B | Terna | CC BY 4.0 (on ENTSO-E's free re-use list) | ✅ yes |
| Day-ahead prices | ENTSO-E Transparency Platform | 12.1.D | Terna | not on the free re-use list | ❌ no, derived results only |
| Actual total load | ENTSO-E Transparency Platform | 6.1.A | Terna | not on the free re-use list | ❌ no, derived results only |
| *Planned:* day-ahead generation forecasts (total, wind & solar) | ENTSO-E Transparency Platform | 14.1.C, 14.1.D | Terna | not on the free re-use list | ❌ no |
| *Planned:* actual generation per production type | ENTSO-E Transparency Platform | 16.1.B&C | Terna | not on the free re-use list | ❌ no |
| *Planned:* imbalance prices and volumes | ENTSO-E Transparency Platform | 17.1.G, 17.1.H | n/a | CC BY 4.0 | ✅ when added |
| *Planned:* cross-border physical flows | ENTSO-E Transparency Platform | 12.1.G | n/a | CC BY 4.0 | ✅ when added |

Licence references:
- ENTSO-E: *List of Data available for free re-use* (last modified 18 October 2023),
  linked from the Transparency Platform's Legal Terms and Conditions page. Listed data is
  released under CC BY 4.0; other data remains subject to the rights of its primary owner.
- Open-Meteo: [terms](https://open-meteo.com/en/terms) and [licence](https://open-meteo.com/en/licence).
  The free API is for non-commercial use.

## Permission requests

| Date | Who | About | Status |
|---|---|---|---|
| 2026-11-13 | ENTSO-E Transparency Platform helpdesk | Reuse of 12.1.D, 6.1.A, 16.1.B&C | Answered 2026-11-16: listed data is CC BY; for the rest, ask the data provider |
| 2026-11-16 | Terna (crm@terna.it) | Publishing 12.1.D, 6.1.A, 14.1.A/C/D, 16.1.B&C, or only aggregates | Waiting for reply |

If Terna grants permission, the relevant exports switch to `public=True` and this table
is updated.

## Data-quality notes from the providers

- **Actual load is provisional.** Terna publishes partly estimated values, revises them the
  next day, and finalises them after about 40 days. The pipeline re-downloads the last two
  months on every run so revisions are picked up.
- **15-minute resolution.** In the ENTSO-E data, Italian day-ahead prices and load are
  hourly until the end of 2024 and in 15-minute intervals from 1 January 2025. The pipeline
  keeps the original resolution in the raw files and averages to hourly in the
  processed tables.
- **Omitted repeated values.** ENTSO-E leaves out a value when it equals the previous one.
  The parser rebuilds every interval (see
  [energy_lab/ingest/entsoe_xml.py](../energy_lab/ingest/entsoe_xml.py)).
