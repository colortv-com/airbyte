#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import base64
import json
import logging
from datetime import datetime
from typing import Any, Iterable, List, Mapping, Optional, Set

import requests
from facebook_business.adobjects.ad import Ad as FBAd
from facebook_business.adobjects.adaccount import AdAccount as FBAdAccount
from facebook_business.adobjects.adcreative import AdCreative as FBAdCreative
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.user import User
from facebook_business.exceptions import FacebookRequestError

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import package_name_from_class
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime, ab_datetime_parse
from source_facebook_marketing.spec import ValidAdSetStatuses, ValidAdStatuses, ValidCampaignStatuses

from .base_insight_streams import AdsInsights
from .base_streams import FBMarketingIncrementalStream, FBMarketingReversedIncrementalStream, FBMarketingStream


logger = logging.getLogger("airbyte")


def fetch_thumbnail_data_url(url: str) -> Optional[str]:
    """Request thumbnail image and return it embedded into the data-link"""
    try:
        response = requests.get(url)
        if response.status_code == requests.status_codes.codes.OK:
            _type = response.headers["content-type"]
            data = base64.b64encode(response.content)
            return f"data:{_type};base64,{data.decode('ascii')}"
        else:
            logger.warning(f"Got {repr(response)} while requesting thumbnail image.")
    except Exception as exc:
        logger.warning(f"Got {str(exc)} while requesting thumbnail image: {url}.")
    return None


class AdCreatives(FBMarketingStream):
    """AdCreative is append-only stream
    doc: https://developers.facebook.com/docs/marketing-api/reference/ad-creative
    """

    entity_prefix = "adcreative"

    def __init__(self, fetch_thumbnail_images: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._fetch_thumbnail_images = fetch_thumbnail_images

    def fields(self, **kwargs) -> List[str]:
        """Remove "thumbnail_data_url" field because it is a computed field, and it's not a field that we can request from Facebook"""
        if self._fields:
            return self._fields

        self._fields = [f for f in super().fields(**kwargs) if f != "thumbnail_data_url"]
        return self._fields

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Read with super method and append thumbnail_data_url if enabled"""
        for record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            if self._fetch_thumbnail_images:
                thumbnail_url = record.get("thumbnail_url")
                if thumbnail_url:
                    record["thumbnail_data_url"] = fetch_thumbnail_data_url(thumbnail_url)
            yield record

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_ad_creatives(params=params, fields=self.fields())


class AdCreativesFromAds(FBMarketingStream):
    """Alternative stream to fetch ad creatives through the ads endpoint.

    This stream fetches creatives by first getting ads (which includes creative IDs),
    then fetching full creative details for each unique creative ID. This approach
    can help avoid the "Please reduce the amount of data you're asking for" error
    that occurs with large accounts when using the direct adcreatives endpoint.

    The two-step approach:
    1. Fetch ads with just the 'creative' field (returns creative ID reference)
    2. For each unique creative ID, fetch full creative details via AdCreative API

    doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup
    related issue: https://github.com/airbytehq/oncall/issues/11128
    """

    entity_prefix = "ad"
    status_field = "effective_status"
    valid_statuses = [status.value for status in ValidAdStatuses]

    def __init__(self, fetch_thumbnail_images: bool = False, **kwargs):
        super().__init__(**kwargs)
        self._fetch_thumbnail_images = fetch_thumbnail_images
        self._seen_creative_ids: Set[str] = set()
        self._creative_fields: Optional[List[str]] = None
        self._fields = ["id", "creative"]

    @property
    def name(self) -> str:
        return "ad_creatives_from_ads"

    def get_json_schema(self) -> Mapping[str, Any]:
        """Use the same schema as ad_creatives stream"""
        loader = ResourceSchemaLoader(package_name_from_class(self.__class__))
        return loader.get_schema("ad_creatives")

    def _get_creative_fields(self) -> List[str]:
        """Get the list of creative fields to request, excluding computed fields"""
        if self._creative_fields:
            return self._creative_fields

        json_schema = self.get_json_schema()
        creative_fields = list(json_schema.get("properties", {}).keys())
        self._creative_fields = [f for f in creative_fields if f not in ("thumbnail_data_url", "account_id")]
        return self._creative_fields

    def fields(self, **kwargs) -> List[str]:
        """Return fields to request from the ads endpoint - just id and creative reference"""
        return self._fields

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_ads(params=params, fields=self.fields())

    def _fetch_creative_details(self, creative_id: str) -> Optional[Mapping[str, Any]]:
        """Fetch full creative details by ID using the AdCreative API"""
        try:
            creative = FBAdCreative(creative_id)
            creative_data = creative.api_get(fields=self._get_creative_fields())
            return creative_data.export_all_data()
        except (FacebookRequestError, TypeError) as e:
            logger.warning(f"Failed to fetch creative {creative_id}: {e}")
            return None

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        """Read ads, extract unique creative IDs, and fetch full creative details"""
        self._seen_creative_ids = set()

        for ad_record in super().read_records(sync_mode, cursor_field, stream_slice, stream_state):
            creative_id = ad_record.get("creative", {}).get("id")
            if not creative_id or creative_id in self._seen_creative_ids:
                continue

            self._seen_creative_ids.add(creative_id)

            creative_data = self._fetch_creative_details(creative_id)
            if not creative_data:
                continue

            self.fix_date_time(creative_data)

            if self._fetch_thumbnail_images:
                thumbnail_url = creative_data.get("thumbnail_url")
                if thumbnail_url:
                    creative_data["thumbnail_data_url"] = fetch_thumbnail_data_url(thumbnail_url)

            self.add_account_id(creative_data, stream_slice["account_id"])
            yield creative_data


class CustomConversions(FBMarketingStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/custom-conversion"""

    entity_prefix = "customconversion"

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_custom_conversions(params=params, fields=self.fields())


class CustomAudiences(FBMarketingStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/custom-audience"""

    entity_prefix = "customaudience"
    # The `rule` field is excluded from the list because it caused the error message "Please reduce the amount of data" for certain connections.
    # https://github.com/airbytehq/oncall/issues/2765
    fields_exceptions = ["rule"]

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_custom_audiences(params=params, fields=self.fields())


class Ads(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/adgroup"""

    entity_prefix = "ad"
    status_field = "effective_status"
    valid_statuses = [status.value for status in ValidAdStatuses]

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_ads(params=params, fields=self.fields())


class AdSets(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign"""

    entity_prefix = "adset"
    status_field = "effective_status"
    valid_statuses = [status.value for status in ValidAdSetStatuses]

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_ad_sets(params=params, fields=self.fields())


class Campaigns(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group"""

    entity_prefix = "campaign"
    status_field = "effective_status"
    valid_statuses = [status.value for status in ValidCampaignStatuses]

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_campaigns(params=params, fields=self.fields())


class Activities(FBMarketingIncrementalStream):
    """doc: https://developers.facebook.com/docs/marketing-api/reference/ad-activity"""

    entity_prefix = "activity"
    cursor_field = "event_time"
    primary_key = None

    def fields(self, **kwargs) -> List[str]:
        """Remove account_id from fields as cannot be requested, but it is part of schema as foreign key, will be added during processing"""
        if self._fields:
            return self._fields

        self._fields = [f for f in super().fields(**kwargs) if f != "account_id"]
        return self._fields

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_activities(fields=self.fields(), params=params)

    def _state_filter(self, stream_state: Mapping[str, Any]) -> Mapping[str, Any]:
        """Additional filters associated with state if any set"""
        state_value = stream_state.get(self.cursor_field)
        if stream_state:
            since = ab_datetime_parse(state_value) if isinstance(state_value, str) else AirbyteDateTime.from_datetime(state_value)
        elif self._start_date:
            since = self._start_date
        else:
            # if start_date is not specified then do not use date filters
            return {}

        potentially_new_records_in_the_past = self._filter_statuses and (
            set(self._filter_statuses) - set(stream_state.get("filter_statuses", []))
        )
        if potentially_new_records_in_the_past:
            self.logger.info(f"Ignoring bookmark for {self.name} because of enabled `filter_statuses` option")
            if self._start_date:
                since = self._start_date
            else:
                # if start_date is not specified then do not use date filters
                return {}

        return {"since": int(since.timestamp())}


class Videos(FBMarketingReversedIncrementalStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/video"""

    entity_prefix = "video"

    def fields(self, **kwargs) -> List[str]:
        """Remove account_id from fields as cannot be requested, but it is part of schema as foreign key, will be added during processing"""
        if self._fields:
            return self._fields

        self._fields = [f for f in super().fields() if f != "account_id"]
        return self._fields

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        # Remove filtering as it is not working for this stream since 2023-01-13
        return self._api.get_account(account_id=account_id).get_ad_videos(params=params, fields=self.fields())


class AdAccount(FBMarketingStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/ad-account"""

    use_batch = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._fields_dict = {}

    def get_task_permissions(self, account_id: str) -> Set[str]:
        """https://developers.facebook.com/docs/marketing-api/reference/ad-account/assigned_users/"""
        res = set()
        me = User(fbid="me", api=self._api.api)
        for business_user in me.get_business_users():
            assigned_users = self._api.get_account(account_id=account_id).get_assigned_users(
                params={"business": business_user["business"].get_id()}
            )
            for assigned_user in assigned_users:
                if business_user.get_id() == assigned_user.get_id():
                    res.update(set(assigned_user["tasks"]))
        return res

    def fields(self, account_id: str, **kwargs) -> List[str]:
        if self._fields_dict.get(account_id):
            return self._fields_dict.get(account_id)

        properties = super().fields(**kwargs)
        # https://developers.facebook.com/docs/marketing-apis/guides/javascript-ads-dialog-for-payments/
        # To access "funding_source_details", the user making the API call must have a MANAGE task permission for
        # that specific ad account.
        permissions = self.get_task_permissions(account_id=account_id)
        if "funding_source_details" in properties and "MANAGE" not in permissions:
            properties.remove("funding_source_details")
        if "is_prepay_account" in properties and "MANAGE" not in permissions:
            properties.remove("is_prepay_account")

        self._fields_dict[account_id] = properties
        return properties

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        """noop in case of AdAccount"""
        fields = self.fields(account_id=account_id)
        try:
            return [FBAdAccount(self._api.get_account(account_id=account_id).get_id()).api_get(fields=fields)]
        except FacebookRequestError as e:
            # This is a workaround for cases when account seem to have all the required permissions
            # but despite that is not allowed to get `owner` field. See (https://github.com/airbytehq/oncall/issues/3167)
            if e.api_error_code() == 200 and e.api_error_message() == "(#200) Requires business_management permission to manage the object":
                fields.remove("owner")
                return [FBAdAccount(self._api.get_account(account_id=account_id).get_id()).api_get(fields=fields)]
            # FB api returns a non-obvious error when accessing the `funding_source_details` field
            # even though user is granted all the required permissions (`MANAGE`)
            # https://github.com/airbytehq/oncall/issues/3031
            if e.api_error_code() == 100 and e.api_error_message() == "Unsupported request - method type: get":
                fields.remove("funding_source_details")
                return [FBAdAccount(self._api.get_account(account_id=account_id).get_id()).api_get(fields=fields)]
            raise e


class Images(FBMarketingReversedIncrementalStream):
    """See: https://developers.facebook.com/docs/marketing-api/reference/ad-image"""

    def list_objects(self, params: Mapping[str, Any], account_id: str) -> Iterable:
        return self._api.get_account(account_id=account_id).get_ad_images(params=params, fields=self.fields(account_id=account_id))

    def get_record_deleted_status(self, record) -> bool:
        return record[AdImage.Field.status] == AdImage.Status.deleted


class AdsInsightsAgeAndGender(AdsInsights):
    breakdowns = ["age", "gender"]


class AdsInsightsCountry(AdsInsights):
    breakdowns = ["country"]


class AdsInsightsRegion(AdsInsights):
    breakdowns = ["region"]


class AdsInsightsDma(AdsInsights):
    breakdowns = ["dma"]


class AdsInsightsPlatformAndDevice(AdsInsights):
    breakdowns = ["publisher_platform", "platform_position", "impression_device"]
    # FB Async Job fails for unknown reason if we set other breakdowns
    # my guess: it fails because of very large cardinality of result set (Eugene K)
    action_breakdowns = ["action_type"]


class AdsInsightsActionType(AdsInsights):
    breakdowns = []
    action_breakdowns = ["action_type"]


class AdsInsightsActionCarouselCard(AdsInsights):
    action_breakdowns = ["action_carousel_card_id", "action_carousel_card_name"]


class AdsInsightsActionConversionDevice(AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsActionProductID(AdsInsights):
    breakdowns = ["product_id"]
    action_breakdowns = []


class AdsInsightsActionReaction(AdsInsights):
    action_breakdowns = ["action_reaction"]


class AdsInsightsActionVideoSound(AdsInsights):
    action_breakdowns = ["action_video_sound"]


class AdsInsightsActionVideoType(AdsInsights):
    action_breakdowns = ["action_video_type"]


class AdsInsightsDeliveryDevice(AdsInsights):
    breakdowns = ["device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsDeliveryPlatform(AdsInsights):
    breakdowns = ["publisher_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsDeliveryPlatformAndDevicePlatform(AdsInsights):
    breakdowns = ["publisher_platform", "device_platform"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsAge(AdsInsights):
    breakdowns = ["age"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsCountry(AdsInsights):
    breakdowns = ["country"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsDMARegion(AdsInsights):
    breakdowns = ["dma"]
    action_breakdowns = ["action_type"]


class AdsInsightsDemographicsGender(AdsInsights):
    breakdowns = ["gender"]
    action_breakdowns = ["action_type"]


def _get_ad_ids_from_insights(api, account_id: str, start_date: Optional[datetime], end_date: Optional[datetime]) -> Set[str]:
    """Fetch all unique ad_ids that appear in insights for a given date range."""
    account = api.get_account(account_id=account_id)
    params: dict = {
        "level": "ad",
        "fields": "ad_id",
        "limit": 500,
    }
    if start_date and end_date:
        since = start_date.isoformat()[:10] if hasattr(start_date, "isoformat") else str(start_date)[:10]
        until = end_date.isoformat()[:10] if hasattr(end_date, "isoformat") else str(end_date)[:10]
        params["time_range"] = json.dumps({"since": since, "until": until})

    ad_ids: Set[str] = set()
    for row in account.get_insights(params=params):
        ad_id = row.get("ad_id")
        if ad_id:
            ad_ids.add(ad_id)

    logger.info(f"InsightsFilter: found {len(ad_ids)} unique ad_ids in insights for account {account_id}")
    return ad_ids


INSIGHTS_BATCH_SIZE = 50


def _fetch_by_ids(api, ids: List[str], fields: str) -> dict:
    """Batch-fetch Facebook objects by IDs (up to 50 per request).

    Uses url_override to call GET /?ids=... which is the multi-ID lookup endpoint.
    """
    base_url = f"https://graph.facebook.com/{api.api.API_VERSION}"
    all_results: dict = {}

    for i in range(0, len(ids), INSIGHTS_BATCH_SIZE):
        batch = ids[i : i + INSIGHTS_BATCH_SIZE]
        try:
            response = api.api.call(
                method="GET",
                path=[],
                params={"ids": ",".join(batch), "fields": fields},
                url_override=base_url,
            )
            all_results.update(response.json())
        except FacebookRequestError as e:
            logger.warning(f"Failed to batch-fetch IDs: {e}")

    return all_results


class AdsFilteredByInsights(Ads):
    """Ads stream that only fetches ads appearing in insights."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        return "ads"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        account_id = stream_slice["account_id"]
        ad_ids = _get_ad_ids_from_insights(self._api, account_id, self._start_date, self._end_date)

        if not ad_ids:
            return

        fields_str = ",".join(self.fields())
        data = _fetch_by_ids(self._api, list(ad_ids), fields_str)

        for ad_id, ad_data in data.items():
            self.fix_date_time(ad_data)
            self.add_account_id(ad_data, account_id)
            yield ad_data


class AdCreativesFilteredByInsights(AdCreatives):
    """AdCreatives stream that only fetches creatives linked to ads in insights."""

    def __init__(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, **kwargs):
        self._insights_start_date = start_date
        self._insights_end_date = end_date
        super().__init__(**kwargs)

    @property
    def name(self) -> str:
        return "ad_creatives"

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        account_id = stream_slice["account_id"]
        ad_ids = _get_ad_ids_from_insights(self._api, account_id, self._insights_start_date, self._insights_end_date)

        if not ad_ids:
            return

        # Step 2: get creative_ids from ads (batch)
        ads_data = _fetch_by_ids(self._api, list(ad_ids), "id,creative{id}")
        creative_ids: Set[str] = set()
        for ad_data in ads_data.values():
            creative = ad_data.get("creative", {})
            cid = creative.get("id")
            if cid:
                creative_ids.add(cid)

        logger.info(f"InsightsFilter: found {len(creative_ids)} unique creatives for account {account_id}")

        if not creative_ids:
            return

        # Step 3: fetch full creative details (batch)
        creative_fields = [f for f in self.fields() if f != "thumbnail_data_url"]
        creatives_data = _fetch_by_ids(self._api, list(creative_ids), ",".join(creative_fields))

        for creative_data in creatives_data.values():
            self.fix_date_time(creative_data)
            self.add_account_id(creative_data, account_id)

            if self._fetch_thumbnail_images:
                thumbnail_url = creative_data.get("thumbnail_url")
                if thumbnail_url:
                    creative_data["thumbnail_data_url"] = fetch_thumbnail_data_url(thumbnail_url)

            yield creative_data
