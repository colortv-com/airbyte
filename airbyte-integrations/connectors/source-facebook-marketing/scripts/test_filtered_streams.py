"""Local smoke test for AdSets/CampaignsFilteredByInsights.

Validates that:
  1. A single insights pass yields all three id sets (ad/adset/campaign).
  2. The module-level cache returns the same object on repeat calls.
  3. Filtered AdSets/Campaigns streams batch-fetch objects via _fetch_by_ids
     and return exactly the IDs collected from insights (no extras).
  4. The filtered set is a strict subset of what the unfiltered stream returns
     over the same window.

Usage:
    cd airbyte-integrations/connectors/source-facebook-marketing
    poetry install
    FB_TOKEN=EAA... FB_ACCOUNT=act_XXXXXXXXXX poetry run python scripts/test_filtered_streams.py
"""

import os
from datetime import datetime, timedelta, timezone

from airbyte_cdk.models import SyncMode

from source_facebook_marketing.api import API
from source_facebook_marketing.streams.streams import (
    AdSets,
    AdSetsFilteredByInsights,
    CampaignsFilteredByInsights,
    _INSIGHTS_ID_CACHE,
    _get_entity_ids_from_insights,
)


def main() -> None:
    token = os.environ["FB_TOKEN"]
    account = os.environ["FB_ACCOUNT"]

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=3)

    api = API(access_token=token, page_size=100)

    print("=" * 70)
    print("STEP 1: one insights pass, expect three id sets")
    print("=" * 70)
    ids = _get_entity_ids_from_insights(api, account, start, end)
    print(f"  ads:       {len(ids['ad_ids'])}")
    print(f"  adsets:    {len(ids['adset_ids'])}")
    print(f"  campaigns: {len(ids['campaign_ids'])}")
    print(f"  sample adset_id:    {next(iter(ids['adset_ids']), None)}")
    print(f"  sample campaign_id: {next(iter(ids['campaign_ids']), None)}")

    print()
    print("=" * 70)
    print("STEP 2: second call must be a cache hit (no extra FB requests)")
    print("=" * 70)
    print(f"  cache size before: {len(_INSIGHTS_ID_CACHE)}")
    ids2 = _get_entity_ids_from_insights(api, account, start, end)
    print(f"  cache size after:  {len(_INSIGHTS_ID_CACHE)}")
    assert ids is ids2, "expected the same dict object back from the cache"
    print("  OK: cache returned the same object")

    print()
    print("=" * 70)
    print("STEP 3: AdSetsFilteredByInsights — record count, sample, id invariant")
    print("=" * 70)
    adsets_stream = AdSetsFilteredByInsights(
        api=api,
        account_ids=[account],
        start_date=start,
        end_date=end,
        filter_statuses=[],
        page_size=100,
    )
    records = list(
        adsets_stream.read_records(
            sync_mode=SyncMode.full_refresh,
            stream_slice={"account_id": account},
        )
    )
    print(f"  records returned: {len(records)}")
    print(f"  expected (from insights): {len(ids['adset_ids'])}")
    if records:
        print("  first record keys:", sorted(records[0].keys()))
        print(
            "  first record (id, name, effective_status):",
            records[0].get("id"),
            records[0].get("name"),
            records[0].get("effective_status"),
        )

    returned_ids = {r["id"] for r in records}
    missing = ids["adset_ids"] - returned_ids
    extra = returned_ids - ids["adset_ids"]
    print(f"  in insights but no record: {len(missing)}  (usually 0; may be deleted adsets)")
    print(f"  in records but not insights: {len(extra)}  (MUST be 0)")
    assert not extra, f"BUG: extra IDs returned that were not in insights: {extra}"

    print()
    print("=" * 70)
    print("STEP 4: CampaignsFilteredByInsights — same invariant")
    print("=" * 70)
    campaigns_stream = CampaignsFilteredByInsights(
        api=api,
        account_ids=[account],
        start_date=start,
        end_date=end,
        filter_statuses=[],
        page_size=100,
    )
    records = list(
        campaigns_stream.read_records(
            sync_mode=SyncMode.full_refresh,
            stream_slice={"account_id": account},
        )
    )
    print(f"  records returned: {len(records)}")
    print(f"  expected (from insights): {len(ids['campaign_ids'])}")
    if records:
        print(
            "  first record (id, name, objective):",
            records[0].get("id"),
            records[0].get("name"),
            records[0].get("objective"),
        )

    returned_ids = {r["id"] for r in records}
    extra = returned_ids - ids["campaign_ids"]
    assert not extra, f"BUG: extra campaign IDs returned: {extra}"

    print()
    print("=" * 70)
    print("STEP 5: every filtered adset_id must exist on the account")
    print("=" * 70)
    # We can't compare against `AdSets` with the same date range — that stream
    # filters by `updated_time`, while insights filter by delivery date. An adset
    # that delivered impressions yesterday but hasn't been edited in months
    # appears in insights, not in `AdSets(start_date=3d_ago)`. So we list ALL
    # adsets on the account (no date filter) and assert the filtered set is a
    # subset of that.
    all_adsets = AdSets(
        api=api,
        account_ids=[account],
        start_date=None,
        end_date=None,
        filter_statuses=[],
        page_size=100,
    )
    all_ids = {
        r["id"]
        for r in all_adsets.read_records(
            sync_mode=SyncMode.full_refresh,
            stream_slice={"account_id": account},
        )
    }
    print(f"  all adsets on account: {len(all_ids)}")
    print(f"  filtered adsets:       {len(ids['adset_ids'])}")
    not_in_all = ids["adset_ids"] - all_ids
    print(f"  filtered but not on account: {len(not_in_all)}  (MUST be 0)")
    assert not not_in_all, (
        f"BUG: filtered returns IDs that don't exist on the account: {not_in_all}"
    )

    print()
    print("All checks passed.")


if __name__ == "__main__":
    main()
