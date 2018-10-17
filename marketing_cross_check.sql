-----Installs -----
select "duplicate_installs" as title, countif(row>1) count
from(
  select install_time, appsflyer_device_id, row_number() over(partition by appsflyer_device_id ) row
  from `appsflyer.appsflyer_webhook` 
  where event_type = "install"
  and timestamp_diff(current_timestamp, install_time, hour)/24 < 1
)

----- Registrations -----

select title, countif(row>1) count
from (
  select customer_user_id, "Webhook" as title,
    row_number() over(partition by customer_user_id) row
  from `appsflyer.appsflyer_webhook` 
  where event_name = "registration_candidate"
  and timestamp_diff(current_timestamp, install_time, hour)/24 < 1
  union all
  select iduser, "Attribution" as title,
    row_number() over(partition by iduser) row
  from `export_medium.AttributionAppsflyer` 
  where event = "Registration"
  and timestamp_diff(current_timestamp, timestamp(timestamp), hour)/24 < 1
)
group by title

----- Applicants -----

select title, countif(row>1) count
from (
  select customer_user_id, "Webhook" as title,
    row_number() over(partition by customer_user_id) row
  from `appsflyer.appsflyer_webhook` 
  where event_name = "applicant"
  and timestamp_diff(current_timestamp, install_time, hour)/24 < 1
  union all
  select iduser, "Attribution" as title,
    row_number() over(partition by iduser) row
  from `export_medium.AttributionAppsflyer` 
  where event = "Applicant"
  and timestamp_diff(current_timestamp, timestamp(timestamp), hour)/24 < 1
)
group by title

----- Spend -----

select * from(
  select source,
    spend-lead(spend) over(partition by source order by type desc) as spend_diff,
    clicks-lead(clicks) over(partition by source order by type desc) as click_diff,
    impressions-lead(impressions) over(partition by source order by type desc) as impressions_diff
  from (
    select sum(spend) spend, sum(clicks) clicks, sum( unique_impressions) impressions, 
      "Facebook Ads" as source, "original" type
    from `facebook_ads.insights_view` 
    where date(date_start) = date_add(current_date, interval -2 day)
    union all
    select sum(cost) spend, sum(clicks) clicks, sum(impressions) impressions,
      "Facebook Ads" as source, "query" type
    from `merlin_query_marketing.spend_online` 
    where date = date_add(current_date, interval -2 day)
    and source = "Facebook Ads"
    union all
    select sum(cost / 1000000) spend, sum(clicks) clicks, sum( impressions) impressions, 
      "Google Adwords" as source, "original" type
    from `adwords.campaign_performance_reports_view` 
    where date(date_start) = date_add(current_date, interval -2 day)
    union all
    select sum(cost) spend, sum(clicks) clicks, sum(impressions) impressions, 
      "Google Adwords" as source, "query" type
    from `merlin_query_marketing.spend_online` 
    where date = date_add(current_date, interval -2 day)
    and source = "Google Adwords"
    union all
    select sum(spend) spend, sum(Swipe_Ups) clicks, sum( Paid_Impressions) impressions, 
      "Snapchat" as source, "original" type
    from `snapchat.Campaigns` 
    where date(Start_time) = date_add(current_date, interval -2 day)
    union all
    select sum(cost) spend, sum(clicks) clicks, sum(impressions) impressions, 
      "Snapchat" as source, "query" type
    from `merlin_query_marketing.spend_online` 
    where date = date_add(current_date, interval -2 day)
    and source = "Snapchat"
  )
)
where spend_diff is not null

