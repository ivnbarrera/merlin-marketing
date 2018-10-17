import pandas as pd
import numpy as np
import datetime
import logging
from tzlocal import get_localzone

# SET LOG #
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s", datefmt="%Y-%m-%d - %H:%M:%S")
LOCAL_TIMEZONE = get_localzone()
# START LOG #
log.info('START')
log.info('Start job at {} {} and {} UTC'.format(
    datetime.datetime.now(), LOCAL_TIMEZONE, datetime.datetime.utcnow()))

# SQL Query
comparison_query = """
with arrays as (
  with days as (
    SELECT day
    -- days from November 11 to current day
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2017-05-07', CURRENT_DATE(), INTERVAL 1 DAY)) AS day
  ),
  sources as (
    -- all sources from jazzHR
    select source from unnest(["Facebook Ads","Google Adwords","Snapchat","Apple Search Ads","App Lovin",'Organic','Other',"Unknown"]) as source
  ),
  platforms as (
    select platform from unnest(["WebMobile","iOS","Android"]) as platform
  )
  select * from days, platforms, sources
),
candidate_source as (
    SELECT
      timestamp, idUser, device_type as platform,
      case
        when lower(real_source) like "%facebook%"  then "Facebook Ads"
        when lower(real_source) like "%google%" then "Google Adwords"
        when lower(real_source) like "%kenshoo%" then "Google Adwords"
        when lower(real_source) like "%snapchat%" then "Snapchat"
        when real_source IN ('Apple Search Ads') then "Apple Search Ads"
        when real_source IN ('applovin_int') then "App Lovin"
        when real_source IN ('','merlin_referrals','merlinjobs.com','Organic') then 'Organic'
        when real_source is null then "Unknown"
        else 'Other'
      end as real_source
    from (
        select timestamp, idUser, device_type, appsFlyer_Id,
        COALESCE(media_source_web_included,media_Source) as real_source,
        row_number() over(partition by idUser order by install_time asc) row
        from `merlin-pro.export_medium.AttributionAppsflyer`
        where event = 'Registration'
    )
    where row = 1
  )
-- Conversations from back-end
,convos as (  
  select chats.idCandidate, min(time.convoTime) as timestamp
  from (
    -- get convos that have at least an employer and candidate response (using intersect distinct)
    select idCandidate, idEmployer, idJob
    from `merlin-pro.merlin_analysis_v2.Chat`
    where idSender = idEmployer
    and type = "text"
    and idEmployer != "6d40d0b6-e9ca-4a11-ad40-42764220f9d9"
    and idJob is not null
    group by idCandidate, idEmployer, idJob 
    intersect distinct
    select idCandidate, idEmployer, idJob
    from `merlin-pro.merlin_analysis_v2.Chat`
    where idSender = idCandidate 
    and type = "text"
    group by idCandidate, idEmployer, idJob
  ) as chats
  left join (
    -- get start of conversation time
    select idCandidate, idEmployer, idJob, min(timeStamp) as convoTime
    from `merlin-pro.merlin_analysis_v2.Chat`
    where idSender != initiator
    and type = "text"
    group by idCandidate, idEmployer, idJob
  ) as time
  on chats.idCandidate = time.idCandidate 
  and chats.idEmployer = time.idEmployer 
  and chats.idJob = time.idJob
  group by idCandidate
)
-- Shortlists from back-end, also the commented line is used in case only shortlists from applications is needed
,shortlist as (
  select min(timestamp) timestamp, idUser
  from `merlin-pro.merlin_analysis_v2.CandidateStatistic` 
  where event = "Shortlist"
  group by idUser
  --and idUser in (select idUser from `merlin_analysis_v2.CandidateStatistic`  where event = "Application")
  )  
select day as date, platform, source as real_source
  ,count(distinct registrations.idUser) as registrations
  ,count(distinct CASE WHEN shortlist.timestamp is not null then registrations.idUser else null end) as shortlists
  ,count(distinct CASE WHEN convos.timestamp is not null then registrations.idUser else null end) as convos
from (
  select arrays.day, arrays.platform, arrays.source, iduser
  from arrays
  left join candidate_source
  on arrays.day = date(candidate_source.timestamp)
  and arrays.platform = candidate_source.platform
  and arrays.source = candidate_source.real_source
) registrations
left join shortlist
on registrations.idUser = shortlist.idUser
left join convos
on registrations.idUser = convos.idCandidate
group by date, platform, real_source
"""


projectid = "merlin-pro"

# Get Data 
try:
    comparison_df = pd.read_gbq(comparison_query, projectid, dialect='standard')
except:
    log.error("error loading data from BQ")
comparison_df.date = pd.to_datetime(comparison_df.date,infer_datetime_format=True)

# Sort and replace NaNs
comparison_df2 = comparison_df
#comparison_df2[['date','platform','real_source']] = comparison_df2[['date','platform','real_source']].replace(np.nan,'Unknown')
comparison_df2 =  comparison_df2.sort_values(by=['date', 'platform','real_source'])

# Totals by day
aggregated = comparison_df2.groupby('date')
#- Registrations
regs_day = aggregated.apply(lambda x: x[(x.real_source != 'Organic') & (x.real_source != 'Unknown')]['registrations'].sum())
regs_day.name = 'registrations_day'
if 'registrations_day' in comparison_df2.columns:
    del comparison_df2['registrations_day']
comparison_df2=comparison_df2.join(regs_day,on='date')

#- Shortlists
regs_day = aggregated.apply(lambda x: x[(x.real_source != 'Organic') & (x.real_source != 'Unknown')]['shortlists'].sum())
regs_day.name = 'shortlists_day'
if 'shortlists_day' in comparison_df2.columns:
    del comparison_df2['shortlists_day']
comparison_df2=comparison_df2.join(regs_day,on='date')

#- Convos
regs_day = aggregated.apply(lambda x: x[(x.real_source != 'Organic') & (x.real_source != 'Unknown')]['convos'].sum())
regs_day.name = 'convos_day'
if 'convos_day' in comparison_df2.columns:
    del comparison_df2['convos_day']
comparison_df2=comparison_df2.join(regs_day,on='date')

# Distributed last 28 days, rolling sum
def get_rolling_amount(grp, measure, freq='28D'):
    return grp.rolling(freq, on='date')[measure].sum()

comparison_df2['registrations_28_days'] = comparison_df2.groupby(['platform','real_source'], as_index=False, group_keys=False) \
                            .apply(get_rolling_amount, measure='registrations')

comparison_df2['shortlists_28_days'] = comparison_df2.groupby(['platform','real_source'], as_index=False, group_keys=False) \
                            .apply(get_rolling_amount, measure='shortlists')

comparison_df2['convos_28_days'] = comparison_df2.groupby(['platform','real_source'], as_index=False, group_keys=False) \
                            .apply(get_rolling_amount, measure='convos')

# Totals last 28 days, rolling sum
def get_rolling_amount_2(grp, measure, freq='28D'):
    by_day = measure + '_day'
    return grp.rolling(freq, on='date')[by_day].sum()

comparison_df2['regs_28_days_total'] = comparison_df2.groupby(['platform','real_source'], as_index=False, group_keys=False) \
                            .apply(get_rolling_amount_2, measure='registrations')

comparison_df2['short_28_days_total'] = comparison_df2.groupby(['platform','real_source'], as_index=False, group_keys=False) \
                            .apply(get_rolling_amount_2, measure='shortlists')

comparison_df2['convos_28_days_total'] = comparison_df2.groupby(['platform','real_source'], as_index=False, group_keys=False) \
                            .apply(get_rolling_amount_2, measure='convos')

# First model included comparison within the same day

#comparison_df2['short_regs'] = comparison_df2.shortlists/comparison_df2.registrations
#comparison_df2['convos_regs'] = comparison_df2.convos/comparison_df2.registrations

#comparison_df2['short_regs_day'] = comparison_df2.shortlists_day/comparison_df2.registrations_day
#comparison_df2['convos_regs_day'] = comparison_df2.convos_day/comparison_df2.registrations_day

comparison_df2['short_regs_28_days'] = comparison_df2.shortlists_28_days/comparison_df2.registrations_28_days
comparison_df2['convos_regs_28_days'] = comparison_df2.convos_28_days/comparison_df2.registrations_28_days

comparison_df2['short_regs_28_total'] = comparison_df2.short_28_days_total/comparison_df2.regs_28_days_total
comparison_df2['convos_regs_28_total'] = comparison_df2.convos_28_days_total/comparison_df2.regs_28_days_total

comparison_df2['shortlist_factor'] = (comparison_df2.short_regs_28_days/comparison_df2.short_regs_28_total).where((comparison_df2.real_source != 'Organic')&(comparison_df2.real_source != 'Unknown'),1)
comparison_df2['convos_factor'] = (comparison_df2.convos_regs_28_days/comparison_df2.convos_regs_28_total).where((comparison_df2.real_source != 'Organic')&(comparison_df2.real_source != 'Unknown'),1)

# Actual calculation of adjusted registrations
try:
    comparison_df3 = comparison_df2.loc[:,['date','platform','real_source','registrations','registrations_day','registrations_28_days','regs_28_days_total','shortlist_factor','convos_factor']]
    comparison_df3['total_factor'] =  comparison_df3.loc[:,['shortlist_factor','convos_factor']].mean(axis=1)
    comparison_df3['adjusted_28_days'] =  comparison_df3.registrations_28_days*comparison_df3.total_factor
    comparison_df3['adjusted_registrations'] = (comparison_df3.registrations*(comparison_df3.adjusted_28_days/comparison_df3.regs_28_days_total)/(comparison_df3.registrations/comparison_df3.registrations_day)).where((comparison_df3.real_source != 'Organic')&(comparison_df3.real_source != 'Unknown'),comparison_df3.registrations) 
except:
    log.error("error calculating adjusted registrations")
    

log.info('Difference  original registrations: {} and adjusted registrations: {}%'.format(
        comparison_df3.registrations.sum(), comparison_df3.adjusted_registrations.sum()))

# WRITE TO BQ 
try:
    bq_destination_path = 'miscellaneous.registrations_adjusted'
    comparison_df3.to_gbq(bq_destination_path, project_id='merlin-pro', if_exists='replace')
except:
    log.error("error writing to BQ")

log.info('End job at {} {} and {} UTC'.format(
        datetime.datetime.now(), LOCAL_TIMEZONE, datetime.datetime.utcnow())
    )
log.info('END')