import pandas as pd
import numpy as np
import sqlite3
import psycopg2
from datetime import date
from datetime import timedelta


def New_Prospect_Pro():

    connection = psycopg2.connect(host="88.198.110.221", port = 54321, database="gamooga", user="caratlane9238", password="W5eB1bustiYo")

    #New code

    df_new = pd.read_sql("""
                            select t.email, t.mobile, t.pincode, t.city, t.tag, t.comments,
                            (case 
                            when t.intent = '1.high' and m.prospect_intent in ('High','Very High') then '1.high'
                            when (t.intent = '1.high' and m.prospect_intent not in ('High','Very High')) or t.intent = '2.medium' then '2.medium'
                            else '3.low'
                            end) as intent
                            from
                            (
                                select c.email, c.mobile, c.pincode, 
                                (case 
                                when (c.city ilike '%Noida%' or c.city ilike '%Gurgaon%' or c.city ilike '%Delhi%') then 'Delhi NCR'
                                when c.city ilike '%Bengaluru%' or c.city ilike '%Bangalore%' then 'Bangalore'
                                when (c.city ilike '%mumbai%' or c.city ilike '%bhiwandi%' or c.city ilike 'kalyan%' or c.city ilike '%Dombivli%' or c.city ilike '%Navi Mumbai%' or c.city ilike '%thane -%' or c.city ilike '%panvel%' or c.city ilike '%virar%' or c.city ilike '%Ulhasnagar') then 'Mumbai MMR'
                                when c.city ilike '%Hyderabad%' then 'Hyderabad'
                                when c.city ilike '%Kolkata%' then 'Kolkata'
                                when c.city ilike '%Chennai%' then 'Chennai' 
                                when c.city ilike '%Pune%' then 'Pune'
                                when (c.city ilike '%Bhubaneswar%' or c.city ilike '%Bhubaneshwar%') then 'Bhubaneswar'
                                when c.city ilike '%Patna%' then 'Patna'
                                when c.city ilike '%Ahmedabad%' then 'Ahmedabad'
                                when c.city ilike '%Lucknow%' then 'Lucknow'
                                when c.city ilike '%Jaipur%' then 'Jaipur'
                                when c.city ilike '%chandigarh%' then 'Chandigarh'
                                else c.city
                                end) as city, c.intent, c.tag, c.comments
                                from
                                (
                                    select t.email, t.mobile, t.pincode, 
                                    (case 
                                    when t.pincode is not null then p.city
                                    when t.pincode is null then t.city
                                    end) as city, t.intent, t.tag,
                                    (
                                        case 
                                        when t.tag = '1.Occasion' and t.nextanniv is not null and t.ptype is not null then concat('Anniversary x ',t.ptype)
                                        when t.tag = '1.Occasion' and t.nextanniv is not null and t.ptype is null then 'Anniversary'
                                        when t.tag = '1.Occasion' and t.nextbday is not null and t.ptype is not null then concat('Birthday x ',t.ptype)
                                        when t.tag = '1.Occasion' and t.nextbday is not null and t.ptype is null then 'Birthday'
                                        when t.tag = '1.Occasion' and t.nextspbday is not null and t.ptype is not null then concat('Spouse Birthday x ',t.ptype)
                                        when t.tag = '1.Occasion' and t.nextspbday is not null and t.ptype is null then concat('Spouse Birthday')
                                        when t.tag in ('2.BRAND NEW','3.Returning60','4.Dormant') and t.ptype is not null then t.ptype
                                        else 'NA'
                                        end
                                    ) as comments
                                    from
                                    (
                                        select t.email, t.phone as mobile, t.pincode, t.city,
                                        (case 
                                        when t.nextbday is not null or t.nextanniv is not null or t.nextspbday is not null then '1.Occasion'
                                        when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.cohort = 'BRAND NEW' then '2.BRAND NEW'
                                        when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.cohort = 'Returning60' then '3.Returning60'
                                        when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.cohort = 'Dormant' then '4.Dormant'
                                        end) as tag, t.intent,
                                        t.nextbday, t.nextanniv, t.nextspbday, t.ptype
                                        from
                                        (
                                            select t.email, t.phone, t.pincode, t.city, t.intent, 
                                            (case 
                                            when bn.vis is not null then 'BRAND NEW'
                                            when bn.vis is null and rt.vis is not null then 'Returning60'
                                            when bn.vis is null and rt.vis is null then 'Dormant'
                                            end) as cohort,
                                            b.nextbday, a.nextanniv, p.nextspbday, pd.ptype
                                            from
                                            (
                                                select 
                                                (case when d.email is not null then d.email else concat(t.phone,'@oneview.com') end) as email,
                                                t.phone, p.pin as pincode, c.city,
                                                t.intent, t.vis
                                                from
                                                (
                                                    select t.phone, t.intent, t.vis
                                                    from
                                                    (
                                                        select right(t.phone,10) as phone, p.intent, t.vis,
                                                        dense_rank() over(partition by right(t.phone,10) order by p.intent) as rank,
                                                        row_number() over(partition by right(t.phone,10) order by p.intent) as rown
                                                        from
                                                        (
                                                                select _visid as vis, lp_tbyb_livemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.lp_tbyb_livesubmitlead 
                                                                union all
                                                                select _visid as vis, lp_tbyb_realimagemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.lp_tbyb_realimagesubmitlead
                                                                union all
                                                                select _visid as vis, pdp_tbyb_livemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.pdp_tbyb_livesubmitlead 
                                                                union all
                                                                select _visid as vis, pdp_tbyb_realimagemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.pdp_tbyb_realimagesubmitlead
                                                                union all
                                                                select _visid as vis, pdp_livemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.PDP_LIVEPincodeLeadSuccess  
                                                                union all
                                                                select _visid as vis, pdp_livemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.PDP_LIVEProductLeadSuccess 
                                                                union all
                                                                select _visid as vis, lp_livemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.LP_LIVEProductLeadSuccess  
                                                                union all
                                                                select _visid as vis, lp_livemobile as phone, _epoch at time zone 'UTC+5:30' as dt,'site_live' as source from gmg_92ca38ca.LP_LIVEPincodeLeadSuccess
                                                                union all
                                                                select _visid as vis, (case when contact_no is null then tah_phone else contact_no end) as phone, _epoch at time zone 'UTC+5:30' as dt,'site_tah' as source from gmg_92ca38ca.tah_placerequest 
                                                                union all
                                                                select _visid as vis, chk_tahmobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_tah' as source from gmg_92ca38ca.chk_tahplacerequest
                                                                union all
                                                                select _visid as vis, xcl_mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'xcl_apps' as source from gmg_92ca38ca.XCL_EnterProfileCompletionSuccess
                                                                union all
                                                                select _visid as vis, xcl_mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'xcl_site' as source from gmg_92ca38ca.ap_xclprofilecompletionsuccess
                                                                union all
                                                                select _visid as vis, chk_encirclephonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_encircle' as source from gmg_92ca38ca.chk_encirclesendotp
                                                                union all
                                                                select _visid as vis, chk_mobile_number as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_otp' as source from gmg_92ca38ca.chk_verifyotp
                                                                union all
                                                                select _visid as vis, pdp_ringsizermobileno as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_pdp_ringsizer' as source from gmg_92ca38ca.pdp_ringsizersubmitotp
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_otp' as source from gmg_92ca38ca.chk_sendotp
                                                                union all
                                                                select _visid as vis, chk_cod_phno as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_otp' as source from gmg_92ca38ca.chk_cod_sendotp
                                                                union all
                                                                select _visid as vis, chk_tahmobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_otp' as source from gmg_92ca38ca.chk_tahotpsuccess
                                                                union all
                                                                select _visid as vis, (case when chk_phonenumber is null then chk_billingphonenumber else chk_phonenumber end) as phone, _epoch at time zone 'UTC+5:30' as dt, 'chk_shipping' as source from gmg_92ca38ca.chk_exitshippingdetails -- where _epoch at time zone 'UTC+5:30' < '2022-04-01'
                                                                union all
                                                                select _visid as vis, sw_phone as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_callback' as source from gmg_92ca38ca.sw_submitcallbackform
                                                                union all
                                                                select _visid as vis, contact_no as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_callback' as source from gmg_92ca38ca.tah_requestcallback
                                                                union all
                                                                select _visid as vis, phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_callback' as source from gmg_92ca38ca.requestcallback
                                                                union all
                                                                select _visid as vis, pdp_customerphone as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_callback' as source from gmg_92ca38ca.pdp_submitrequestcallback
                                                                union all
                                                                select _visid as vis, pdp_phone as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_callback' as source from gmg_92ca38ca.pdp_submitcallbackform
                                                                union all
                                                                select _visid as vis, ldp_pop_mobileno as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_callback' as source from gmg_92ca38ca.ldp_pop_submitcallback_request
                                                                union all
                                                                select _visid as vis, sw_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_signup' as source from gmg_92ca38ca.sw_signup
                                                                union all
                                                                select _visid as vis, pdp_notifiymemobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'site_notify' as source from gmg_92ca38ca.pdp_notifiyme -- where _visid = 'A4D3s4-njQRsTGZz'
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.fastdelivery_24hrs_popup
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.fastdelivery_soon_popup
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.tah_location_based
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.store_nearestpincode
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.multiplecart
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.giftpage_popup
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.gift_kidpage_popup
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.birthday_samemonth
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.birthday_nextmonth_code
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.anniversary_samemonth_code
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.anniversarynextmonth
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.spousebirthday_samemonth
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.search_popup
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.wishlist_others
                                                                union all
                                                                select _visid as vis, mobile as phone, _epoch at time zone 'UTC+5:30' as dt, 'intervention' as source from gmg_92ca38ca.solitaireoffer	
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_continue
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_send_otp 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_verify_mobile_otp 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_proceed_to_payment 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_change_number 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_social_login 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_change_email 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_signin_by_email 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_resend_otp 
                                                                union all
                                                                select _visid as vis, chk_phonenumber as phone, _epoch at time zone 'UTC+5:30' as dt, 'alibaba' as source from gmg_92ca38ca.alibaba_change_email_pop_up
                                                        )t
                                                        join
                                                        (
                                                            select t.vis, 
                                                            (case
                                                            when t.newmodel_intent = '1.high' or t.oldmodel_intent = '1.high' then '1.high'
                                                            when t.newmodel_intent = '2.medium' and t.oldmodel_intent is null then '2.medium'
                                                            when t.newmodel_intent = '3.low' and t.oldmodel_intent is null then '3.low'
                                                            end) as intent 
                                                            from
                                                            (
                                                                select 
                                                                (case when n.vis is null then o.vis else n.vis end) as vis, 
                                                                n.type as newmodel_intent,
                                                                o.type as oldmodel_intent
                                                                from
                                                                (
                                                                    select t.vis,
                                                                    (case 
                                                                    when t.shortlistingintent = 'high' or t.callintent = 'high' or t.leadintent = 'high' or t.repeatvisit = 'high' or t.giftingintent = 'high' or t.buyingintent = 'high' or t.trialintent = 'high' or t.deliveryintent = 'high' then '1.high'
                                                                    when (t.shortlistingintent = 'low' or t.shortlistingintent is null) and (t.callintent = 'low' or t.leadintent is null) and (t.leadintent = 'low' or t.callintent is null) and (t.repeatvisit = 'low' or t.repeatvisit is null) and (t.trialintent = 'low' or t.trialintent is null) and (t.deliveryintent = 'low' or t.deliveryintent is null) and (t.buyingintent = 'low' or t.buyingintent is null) and (t.giftingintent = 'no' or t.giftingintent is null) then '3.low'
                                                                    else '2.medium'
                                                                    end) as type, 'new_model' as model
                                                                    from
                                                                    (
                                                                        select t.vis,
                                                                        (case when s.ptype in ('P1','P2') then 'high' else 'low' end) as shortlistingintent,
                                                                        clr.callintentscore,clr.callintent,
                                                                        clr.leadintentscore, clr.leadintent, 
                                                                        clr.repeatvisitscore, clr.repeatvisit,
                                                                        gbdt.bdayscore, gbdt.annivscore, gbdt.spousebdayscore, gbdt.engagementscore, 
                                                                        gbdt.giftingbannerscore, gbdt.giftingintent, 
                                                                        gbdt.deliveryintentscore, gbdt.deliveryintent, 
                                                                        gbdt.buyingintentscore,gbdt.buyingintent, 
                                                                        gbdt.trialintentscore, gbdt.trialintent
                                                                        from
                                                                        (
                                                                            select distinct(t.vis) as vis
                                                                            from
                                                                            (
                                                                                select _visid as vis from gmg_92ca38ca.slcron_new where date(_epoch at time zone 'UTC+5:30') = current_date union all -- shortlisting intent dataset id for the date mentioned in line #77
                                                                                select _visid as vis from gmg_92ca38ca.clrcron_new where date(_epoch at time zone 'UTC+5:30') = current_date union all -- clr intent dataset id for the date mentioned in line #77
                                                                                select _visid as vis from gmg_92ca38ca.gbdtcron_new where date(_epoch at time zone 'UTC+5:30') = current_date -- gbdt intent dataset id for the date mentioned in line #77
                                                                            )t
                                                                        )t
                                                                        left join gmg_92ca38ca.slcron_new s on s._visid = t.vis and date(s._epoch at time zone 'UTC+5:30') = current_date-- shortlisting intent dataset id for the date mentioned in line #77
                                                                        left join gmg_92ca38ca.clrcron_new clr on clr._visid = t.vis and date(clr._epoch at time zone 'UTC+5:30') = current_date -- clr intent dataset id for the date mentioned in line #77
                                                                        left join gmg_92ca38ca.gbdtcron_new gbdt on gbdt._visid = t.vis and date(gbdt._epoch at time zone 'UTC+5:30') = current_date -- gbdt intent dataset id for the date mentioned in line #77
                                                                    )t
                                                                )n
                                                                full outer join
                                                                (
                                                                    select t.vis, 
                                                                    (case 
                                                                    when t.buying_score = 'Y' or t.delivery_score = 'High' or t.trial_score = 'High' then '1.high'
                                                                    when t.buying_score = 'N' and (t.delivery_score = 'None' or t.delivery_score = 'Low') and (t.trial_score =  'None' or t.trial_score = 'Low') then '3.low'
                                                                    end) as type, 'old_model' as model
                                                                    from
                                                                    (
                                                                        select _visid as vis, buying_score, delivery_score, trial_score
                                                                        from gmg_92ca38ca.buying_intent 
                                                                        where date(_epoch at time zone 'UTC+5:30') = current_date 
                                                                    )t
                                                                )o on o.vis = n.vis
                                                            )t
                                                        )p on p.vis = t.vis
                                                    )t
                                                    where t.rank = 1 and t.rown = 1 -- and right(t.phone,10) = '413876413'
                                                )t
                                                left join gmg_92ca38ca.ds_data_9505 d on d.vis = t.vis -- right(d.mobile,10) = right(t.phone,10)
                                                left join gmg_92ca38ca.ds_data_10215 r on right(r.mobile,10) = right(t.phone,10) --repeat_upto has to be changed at the start of every month
                                                left join gmg_92ca38ca.ds_data_10215 e on lower(e.email) = lower(d.email) --repeat_upto has to be changed at the start of every month -- select * from gmg_92ca38ca.ds_data_9505 where vis = 'om2XT4SlX2fnzJ47'
                                                left join gmg_92ca38ca.ds_data_9465 b on b.vis = t.vis -- remove the blacklisted visids
                                                left join gmg_92ca38ca.ds_data_9447 s on right(s.phone,10) = right(r.mobile,10) -- remove store nos
                                                left join
                                                (
                                                    select t._visid as vis, t.pin
                                                    from
                                                    (
                                                        select t._visid, t.pin,
                                                        dense_rank() over(partition by t._visid order by t.dt desc) as rank,
                                                        row_number() over(partition by t._visid order by t.dt desc) as rown
                                                        from
                                                        (
                                                            select _visid, pin, cast(dt as date) as dt
                                                            from gmg_92ca38ca.pincode_daily
                                                        )t
                                                    )t
                                                    where t.rank = 1 and t.rown = 1 -- and t._visid = 'om2XT4SlX2fnzJ47'
                                                )p on p.vis = t.vis
                                                left join
                                                (
                                                    select t.vis, reverse(substring(reverse(t.city),position('-' in reverse(t.city))+1, length(reverse(t.city)))) as city
                                                    from
                                                    (
                                                        select s.city, 
                                                        substring(s.city, 1,position('-' in s.city)-2) as derived_city,
                                                        substring(s.city,position(',' in s.city)+1,length(s.city)) as country, s.Dt as dt,s.vis as vis,s.rank
                                                        from
                                                        (
                                                            select _visid as vis,(_epoch at time zone 'UTC+5:30') as Dt,location as city,
                                                            dense_rank() over(partition by _visid order by (_epoch at time zone 'UTC+5:30') desc ) as rank,
                                                            row_number() over(partition by _visid order by (_epoch at time zone 'UTC+5:30') desc ) as rown
                                                            from gmg_92ca38ca.started_session
                                                            where date(_epoch at time zone 'UTC+5:30') between date(current_date)-90 and date(current_date)
                                                            order by 1,3
                                                        ) as s
                                                        where s.rank = 1 and s.rown = 1
                                                    )t
                                                    where t.country = ' India' and t.derived_city <> '?' -- and t.vis = 'om2XT4SlX2fnzJ47' -- select * from gmg_92ca38ca.started_session where _visid = 'om2XT4SlX2fnzJ47'
                                                )c on c.vis = t.vis
                                                where r.email is null and e.email is null and b.vis is null and s.phone is null
                                            )t
                                            left join -- BRAND NEW LOGIC BLOCK
                                            (
                                                select t.vis, t.firstvisitdt
                                                from
                                                (		
                                                        select t.vis, date(min(t.dt)) as firstvisitdt
                                                        from
                                                        (
                                                            select _visid as vis, _epoch at time zone 'UTC+5:30' as dt
                                                            from gmg_92ca38ca.hp_viewpage 
                                                            union all
                                                            select _visid as vis, _epoch at time zone 'UTC+5:30' as dt
                                                            from gmg_92ca38ca.lp_viewpage
                                                            union all
                                                            select _visid as vis, _epoch at time zone 'UTC+5:30' as dt
                                                            from gmg_92ca38ca.pdp_viewproduct
                                                        )t
                                                        group by 1
                                                )t
                                                where date(t.firstvisitdt) between '2023-12-01' and current_date -- to be changed on the 1st of every month
                                            )bn on bn.vis = t.vis
                                            left join -- RETURNING 60 LOGIC BLOCK
                                            (
                                                select distinct(t.vis) as vis
                                                from
                                                (
                                                    select distinct(_visid) as vis from gmg_92ca38ca.hp_viewpage where date(_epoch at time zone 'UTC+5:30') between '2023-10-01' and '2023-11-30' union all
                                                    select distinct(_visid) as vis from gmg_92ca38ca.lp_viewpage where date(_epoch at time zone 'UTC+5:30') between '2023-10-01' and '2023-11-30' union all
                                                    select distinct(_visid) as vis from gmg_92ca38ca.pdp_viewproduct where date(_epoch at time zone 'UTC+5:30') between '2023-10-01' and '2023-11-30' union all
                                                    select distinct(_visid) as vis from gmg_92ca38ca.buying_intent where date(_epoch at time zone 'UTC+5:30') between date('2023-10-01')+1 and date('2023-10-30')+1 and (buying_score = 'Y' or delivery_score = 'High' or trial_score = 'High') union all
                                                    select distinct(t.vis) as vis
                                                    from
                                                    (
                                                        select _visid as vis from gmg_92ca38ca.shortlisting_cron where date(_epoch at time zone 'UTC+5:30') between date('2023-10-01')+1 and date('2023-11-30')+1   union all
                                                        select _visid as vis from gmg_92ca38ca.gbdt_cron where date(_epoch at time zone 'UTC+5:30') between date('2023-10-01')+1 and date('2023-11-30')+1 and (buyingintent = 'high' or deliveryintent = 'high' or trialintent = 'high' or giftingintent = 'high') union all
                                                        select _visid as vis from gmg_92ca38ca.clr_cron where date(_epoch at time zone 'UTC+5:30') between date('2023-10-01')+1 and date('2023-11-30')+1  and (callintent = 'high' or leadintent = 'high' or repeatvisit = 'high') 			
                                                    )t			
                                                )t
                                            )rt on rt.vis = t.vis
                                            left join
                                            (
                                                select t.email, t.nextbday, t.occasion as days_to_occasion
                                                from
                                                (
                                                    select t.email, to_char(t.next_bday,'MON-DD') as nextbday,
                                                    (case 
                                                    when t.dif >= 350 then cast(t.dif-365 as text)
                                                    else cast(t.dif as text)
                                                    end) as occasion
                                                    from
                                                    (
                                                        select t.email, t.bday, t.next_bday, t.next_bday-current_date as dif
                                                        from
                                                        (
                                                            select t.email, t.bday, t.next_bday, t.next_bday-30 as startdt, t.next_bday+15 as enddt
                                                            from
                                                            (
                                                                select t.email as email, t.bday,
                                                                cast(date(t.bday) + ((extract(year from age(date(current_date), date(t.bday))) + 1) * interval '1' year) as date) as next_bday
                                                                from 
                                                                (
                                                                    select t.email, t.bday
                                                                    from
                                                                    (
                                                                        select t.email, t.bday, 
                                                                        dense_rank() over(partition by t.email order by t.dt desc) as rank,
                                                                        row_number() over(partition by t.email order by t.dt desc) as rown
                                                                        from
                                                                        (
                                                                            select lower(customer_email) as email, birth_date as bday, (_epoch at time zone 'UTC+5:30') as dt
                                                                            from gmg_92ca38ca.bday_anniv_automate_dump
                                                                            where birth_date <> 'NULL' and birth_date is not null and birth_date <> '0000-00-00' 
                                                                            and length(birth_date) = '10' -- and birth_month in ('9','10','11')
                                                                            -- and customer_email = 'ashutosh.sharma06@gmail.com'
                                                                            union all
                                                                            select lower(email) as email, birth_date as bday, (_epoch at time zone 'UTC+5:30') as dt
                                                                            from gmg_92ca38ca.domo_occasion_data
                                                                            where birth_date <> 'NULL' and birth_date is not null and birth_date <> '0000-00-00' 
                                                                            and length(birth_date) = '10' -- and birth_month in ('9','10','11')
                                                                            -- and email = 'ashutosh.sharma06@gmail.com'
                                                                        )t
                                                                    )t
                                                                    where t.rank = 1 and t.rown = 1				
                                                                )t
                                                            )t
                                                        )t
                                                    )t
                                                )t
                                                where cast(t.occasion as numeric) >= 0 and cast(t.occasion as numeric) <= 30 -- and t.email = 'tanya.a.marwah@gmail.com'
                                            )b on b.email = t.email
                                            left join
                                            (
                                                select t.email, t.nextanniv, t.occasion as days_to_occasion
                                                from
                                                (
                                                    select t.email, to_char(t.next_anniv,'MON-DD') as nextanniv,
                                                    (case 
                                                    when t.dif >= 350 then cast(t.dif-365 as text)
                                                    else cast(t.dif as text)
                                                    end) as occasion
                                                    from
                                                    (
                                                        select t.email, t.anniv, t.next_anniv, t.next_anniv-current_date as dif
                                                        from
                                                        (
                                                            select t.email, t.anniv, t.next_anniv, t.next_anniv-30 as startdt, t.next_anniv+15 as enddt
                                                            from
                                                            (
                                                                select t.email as email, t.anniv,
                                                                cast(date(t.anniv) + ((extract(year from age(date(current_date), date(t.anniv))) + 1) * interval '1' year) as date) as next_anniv
                                                                from 
                                                                (
                                                                    select t.email, t.anniv
                                                                    from
                                                                    (
                                                                        select t.email, t.anniv, 
                                                                        dense_rank() over(partition by t.email order by t.dt desc) as rank,
                                                                        row_number() over(partition by t.email order by t.dt desc) as rown
                                                                        from
                                                                        (
                                                                            select lower(customer_email) as email, anniversary_date as anniv, (_epoch at time zone 'UTC+5:30') as dt
                                                                            from gmg_92ca38ca.bday_anniv_automate_dump
                                                                            where anniversary_date <> 'NULL' and anniversary_date is not null and anniversary_date <> '0000-00-00' 
                                                                            and length(anniversary_date) = '10' -- and birth_month in ('9','10','11')
                                                                            -- and customer_email = 'ashutosh.sharma06@gmail.com'
                                                                            union all
                                                                            select lower(email) as email, anniversary_date as anniv, (_epoch at time zone 'UTC+5:30') as dt
                                                                            from gmg_92ca38ca.domo_occasion_data
                                                                            where anniversary_date <> 'NULL' and anniversary_date is not null and anniversary_date <> '0000-00-00' 
                                                                            and length(anniversary_date) = '10' -- and birth_month in ('9','10','11')
                                                                            -- and email = 'ashutosh.sharma06@gmail.com'
                                                                        )t
                                                                    )t
                                                                    where t.rank = 1 and t.rown = 1				
                                                                )t
                                                            )t
                                                        )t
                                                    )t
                                                )t
                                                where cast(t.occasion as numeric) >= 0 and cast(t.occasion as numeric) <= 30 -- and t.email = 'tanya.a.marwah@gmail.com'
                                            )a on a.email = t.email 
                                            left join
                                            (
                                                select t.email, t.nextspbday, t.occasion as days_to_occasion
                                                from
                                                (
                                                    select t.email, to_char(t.next_spbday,'MON-DD') as nextspbday,
                                                    (case 
                                                    when t.dif >= 350 then cast(t.dif-365 as text)
                                                    else cast(t.dif as text)
                                                    end) as occasion
                                                    from
                                                    (
                                                        select t.email, t.spbday, t.next_spbday, t.next_spbday-current_date as dif
                                                        from
                                                        (
                                                            select t.email, t.spbday, t.next_spbday, t.next_spbday-30 as startdt, t.next_spbday+15 as enddt
                                                            from
                                                            (
                                                                select t.email as email, t.spbday,
                                                                cast(date(t.spbday) + ((extract(year from age(date(current_date), date(t.spbday))) + 1) * interval '1' year) as date) as next_spbday
                                                                from 
                                                                (
                                                                    select t.email, t.spbday
                                                                    from
                                                                    (
                                                                        select t.email, t.spbday, 
                                                                        dense_rank() over(partition by t.email order by t.dt desc) as rank,
                                                                        row_number() over(partition by t.email order by t.dt desc) as rown
                                                                        from
                                                                        (
                                                                            select lower(customer_email) as email, spouse_birthdate as spbday, (_epoch at time zone 'UTC+5:30') as dt
                                                                            from gmg_92ca38ca.bday_anniv_automate_dump
                                                                            where spouse_birthdate <> 'NULL' and spouse_birthdate is not null and spouse_birthdate <> '0000-00-00' 
                                                                            and length(spouse_birthdate) = '10' -- and birth_month in ('9','10','11')
                                                                            -- and customer_email = 'ashutosh.sharma06@gmail.com'
                                                                            union all
                                                                            select lower(email) as email, spouse_birthday as spbday, (_epoch at time zone 'UTC+5:30') as dt
                                                                            from gmg_92ca38ca.domo_occasion_data
                                                                            where spouse_birthday <> 'NULL' and spouse_birthday is not null and spouse_birthday <> '0000-00-00' 
                                                                            and length(spouse_birthday) = '10' -- and birth_month in ('9','10','11')
                                                                            -- and email = 'ashutosh.sharma06@gmail.com'
                                                                        )t
                                                                    )t
                                                                    where t.rank = 1 and t.rown = 1				
                                                                )t
                                                            )t
                                                        )t
                                                    )t
                                                )t
                                                where cast(t.occasion as numeric) >= 0 and cast(t.occasion as numeric) <= 30 -- and t.email = 'tanya.a.marwah@gmail.com'
                                            )p on p.email = t.email 
                                            left join
                                            (
                                                select t.vis, t.ptype
                                                from
                                                (
                                                    select t.vis, t.ptype, t.skucnt,
                                                    dense_rank() over(partition by t.vis order by t.skucnt desc) as rank,
                                                    row_number() over(partition by t.vis order by t.skucnt desc) as rown
                                                    from
                                                    (
                                                        select p._visid as vis, pdp_producttype as ptype, count(distinct pdp_sku) as skucnt
                                                        from gmg_92ca38ca.pdp_viewproduct p
                                                        where date(_epoch at time zone 'UTC+5:30') between date(current_date)-21 and date(current_date)-1
                                                        group by 1,2
                                                    )t
                                                )t
                                                where t.rank = 1 and t.rown = 1 
                                            )pd on pd.vis = t.vis
                                            where t.pincode is not null or t.city is not null
                                            -- where right(t.phone,10) = '413876413'
                                            -- where t.email = 'anurudh.200214@gmail.com'
                                        )t
                                    )t
                                    left join gmg_92ca38ca.ds_data_9588 p on p.pincode = t.pincode
                                    where t.mobile is not null
                                )c
                            )t
                            join gmg_92ca38ca.ml_prospect_new m on right(m.phone,10) = right(t.mobile,10) and date(m._epoch at time zone 'UTC+5:30') = current_date
                            -- join gmg_92ca38ca.ml_prospect_new m on m.email = t.email and date(m._epoch at time zone 'UTC+5:30') = current_date

                            -- select * from gmg_92ca38ca.ml_prospect_new limit 10
                            """, con=connection)


    return df_new





def Repeat_Prospect_Pro():

    connection = psycopg2.connect(host="88.198.110.221", port = 54321, database="gamooga", user="caratlane9238", password="W5eB1bustiYo")

    # Repeat code

    df_repeat = pd.read_sql("""
                                            -- select distinct(prospect_intent) from gmg_92ca38ca.ml_prospect_repeat limit 10
                                            -- select * from gmg_92ca38ca.ml_prospect_repeat limit 10

                                            select t.email, t.mobile, t.pincode, t.city, t.tag, t.comments,
                                            (case 
                                            when t.intent = '1.high' and m.prospect_intent in ('High','Very High') then '1.high'
                                            when (t.intent = '1.high' and m.prospect_intent not in ('High','Very High')) or t.intent = '2.medium' then '2.medium'
                                            else '3.low'
                                            end) as intent
                                            from
                                            (
                                                select t.email, t.mobile, t.pincode, 
                                                (case 
                                                when t.pincode is not null then p.city
                                                when t.pincode is null then t.city
                                                end) as city, t.intent, t.tag,
                                                (
                                                    case 
                                                    when t.tag = '1.Occasion' and t.nextanniv is not null and t.ptype is not null then concat('Anniversary x ',t.ptype)
                                                    when t.tag = '1.Occasion' and t.nextanniv is not null and t.ptype is null then 'Anniversary'
                                                    when t.tag = '1.Occasion' and t.nextbday is not null and t.ptype is not null then concat('Birthday x ',t.ptype)
                                                    when t.tag = '1.Occasion' and t.nextbday is not null and t.ptype is null then 'Birthday'
                                                    when t.tag = '1.Occasion' and t.nextspbday is not null and t.ptype is not null then concat('Spouse Birthday x ',t.ptype)
                                                    when t.tag = '1.Occasion' and t.nextspbday is not null and t.ptype is null then concat('Spouse Birthday')
                                                    when t.tag = '2.xCLusive' and t.ptype is not null then concat('Balance: ',t.xcl_balance, ' x ', t.ptype)
                                                    when t.tag = '2.xCLusive' and t.ptype is null then concat('Balance: ',t.xcl_balance)
                                                    when t.tag in ('3.Order60','4.Returning60','5.Dormant') and t.ptype is not null then t.ptype
                                                    else 'NA'
                                                    end
                                                ) as comments
                                                from
                                                (
                                                    select t.email, t.mobile, t.pincode, t.city,
                                                    (case 
                                                    when t.nextbday is not null or t.nextanniv is not null or t.nextspbday is not null then '1.Occasion'
                                                    when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.xcl_balance is not null then '2.xCLusive'
                                                    when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.xcl_balance is null and t.cohort = 'Order60' then '3.Order60'
                                                    when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.xcl_balance is null and t.cohort = 'Returning60' then '4.Returning60'
                                                    when t.nextbday is null and t.nextanniv is null and t.nextspbday is null and t.xcl_balance is null and t.cohort = 'Dormant' then '5.Dormant'
                                                    end) as tag, t.intent,
                                                    t.nextbday, t.nextanniv, t.nextspbday, t.xcl_balance, t.ptype
                                                    from
                                                    (
                                                        select t.email, t.mobile, t.pincode, t.city, t.cohort, t.lastorderdt, t.intent,
                                                        b.nextbday, a.nextanniv, p.nextspbday, x.xcl_balance, pd.ptype
                                                        from
                                                        (
                                                            select t.email, t.mobile, t.pincode, t.city, t.lastorderdt, t.intent,
                                                            (case 
                                                            when t.cohort in ('2_Ordered_60_Repeat','1_Ordered_60_First_time') then 'Order60'
                                                            when t.cohort in ('3_Returning_Prospect_60','4_Returning_Active_60') then 'Returning60'
                                                            when t.cohort in ('6_Dormant','5_Ordered_LastYear') then 'Dormant'
                                                            end) as cohort
                                                            from
                                                            (
                                                                select t.email, t.mobile, t.pincode, t.city, t.cohort, t.lastorderdt, t.intent,
                                                                (case when date(t.lastorderdt) < date(current_date)-21 or t.lastorderdt is null then 'y' else 'n' end) as consider
                                                                from
                                                                (
                                                                    select t.email, t.intent, r.mobile, r.pincode, r.city,
                                                                    replace(replace(cast(r.cohort as text),'{',''),'}','') as cohort, l.lastorderdt
                                                                    from
                                                                    (
                                                                        select t.email as email, t.intent
                                                                        from
                                                                        (
                                                                            select lower(d.email) as email, t.intent, 
                                                                            dense_rank() over(partition by lower(d.email) order by t.intent) as rank,
                                                                            row_number() over(partition by lower(d.email) order by t.intent) as rown
                                                                            from
                                                                            (
                                                                                select t.vis, 
                                                                                (case
                                                                                when t.newmodel_intent = '1.high' or t.oldmodel_intent = '1.high' then '1.high'
                                                                                when t.newmodel_intent = '2.medium' and t.oldmodel_intent is null then '2.medium'
                                                                                when t.newmodel_intent = '3.low' and t.oldmodel_intent is null then '3.low'
                                                                                end) as intent 
                                                                                from
                                                                                (
                                                                                    select 
                                                                                    (case when n.vis is null then o.vis else n.vis end) as vis, 
                                                                                    n.type as newmodel_intent,
                                                                                    o.type as oldmodel_intent
                                                                                    from
                                                                                    (
                                                                                        select t.vis,
                                                                                        (case 
                                                                                        when t.shortlistingintent = 'high' or t.callintent = 'high' or t.leadintent = 'high' or t.repeatvisit = 'high' or t.giftingintent = 'high' or t.buyingintent = 'high' or t.trialintent = 'high' or t.deliveryintent = 'high' then '1.high'
                                                                                        when (t.shortlistingintent = 'low' or t.shortlistingintent is null) and (t.callintent = 'low' or t.leadintent is null) and (t.leadintent = 'low' or t.callintent is null) and (t.repeatvisit = 'low' or t.repeatvisit is null) and (t.trialintent = 'low' or t.trialintent is null) and (t.deliveryintent = 'low' or t.deliveryintent is null) and (t.buyingintent = 'low' or t.buyingintent is null) and (t.giftingintent = 'no' or t.giftingintent is null) then '3.low'
                                                                                        else '2.medium'
                                                                                        end) as type, 'new_model' as model
                                                                                        from
                                                                                        (
                                                                                            select t.vis,
                                                                                            (case when s.ptype in ('P1','P2') then 'high' else 'low' end) as shortlistingintent,
                                                                                            clr.callintentscore,clr.callintent,
                                                                                            clr.leadintentscore, clr.leadintent, 
                                                                                            clr.repeatvisitscore, clr.repeatvisit,
                                                                                            gbdt.bdayscore, gbdt.annivscore, gbdt.spousebdayscore, gbdt.engagementscore, 
                                                                                            gbdt.giftingbannerscore, gbdt.giftingintent, 
                                                                                            gbdt.deliveryintentscore, gbdt.deliveryintent, 
                                                                                            gbdt.buyingintentscore,gbdt.buyingintent, 
                                                                                            gbdt.trialintentscore, gbdt.trialintent
                                                                                            from
                                                                                            (
                                                                                                select distinct(t.vis) as vis
                                                                                                from
                                                                                                (
                                                                                                    select _visid as vis from gmg_92ca38ca.slcron_new where date(_epoch at time zone 'UTC+5:30') = current_date union all -- shortlisting intent dataset id for the date mentioned in line #77
                                                                                                    select _visid as vis from gmg_92ca38ca.clrcron_new where date(_epoch at time zone 'UTC+5:30') = current_date union all -- clr intent dataset id for the date mentioned in line #77
                                                                                                    select _visid as vis from gmg_92ca38ca.gbdtcron_new where date(_epoch at time zone 'UTC+5:30') = current_date -- gbdt intent dataset id for the date mentioned in line #77
                                                                                                )t
                                                                                            )t
                                                                                            left join gmg_92ca38ca.slcron_new s on s._visid = t.vis and date(s._epoch at time zone 'UTC+5:30') = current_date-- shortlisting intent dataset id for the date mentioned in line #77
                                                                                            left join gmg_92ca38ca.clrcron_new clr on clr._visid = t.vis and date(clr._epoch at time zone 'UTC+5:30') = current_date -- clr intent dataset id for the date mentioned in line #77
                                                                                            left join gmg_92ca38ca.gbdtcron_new gbdt on gbdt._visid = t.vis and date(gbdt._epoch at time zone 'UTC+5:30') = current_date -- gbdt intent dataset id for the date mentioned in line #77
                                                                                        )t
                                                                                    )n
                                                                                    full outer join
                                                                                    (
                                                                                        select t.vis, 
                                                                                        (case 
                                                                                        when t.buying_score = 'Y' or t.delivery_score = 'High' or t.trial_score = 'High' then '1.high'
                                                                                        when t.buying_score = 'N' and (t.delivery_score = 'None' or t.delivery_score = 'Low') and (t.trial_score =  'None' or t.trial_score = 'Low') then '3.low'
                                                                                        end) as type, 'old_model' as model
                                                                                        from
                                                                                        (
                                                                                            select _visid as vis, buying_score, delivery_score, trial_score
                                                                                            from gmg_92ca38ca.buying_intent 
                                                                                            where date(_epoch at time zone 'UTC+5:30') = current_date 
                                                                                        )t
                                                                                    )o on o.vis = n.vis
                                                                                )t
                                                                            )t
                                                                            join gmg_92ca38ca.ds_data_9505 d on d.vis = t.vis
                                                                            left join gmg_92ca38ca.ds_data_9465 b on b.vis = t.vis -- remove the blacklisted visids
                                                                            where b.vis is null 
                                                                        )t
                                                                        where t.rank = 1 and t.rown = 1 -- and t.email = 'tanya.a.marwah@gmail.com'
                                                                    )t
                                                                    join 
                                                                    (
                                                                        select lower(email) as email, mobile, pincode, city, array_agg(distinct cohort_2) as cohort
                                                                        from gmg_92ca38ca.ds_data_10215 -- repeat upto has to be changed start of every month
                                                                        -- where email = 'tanya.a.marwah@gmail.com'
                                                                        group by 1,2,3,4
                                                                    )r on r.email = t.email
                                                                    left join
                                                                    (
                                                                        select lower(bill_email) as email, max(cast(trans_date as date)) as lastorderdt
                                                                        from gmg_92ca38ca.ds_data_7358 
                                                                        -- where bill_email ilike 'tanya.a.marwah@gmail.com' 
                                                                        group by 1			
                                                                    )l on l.email = t.email
                                                                    left join gmg_92ca38ca.ds_data_9447 s on right(s.phone,10) = right(r.mobile,10) -- remove store nos
                                                                    where s.phone is null
                                                                )t
                                                            )t
                                                            where t.consider = 'y' -- and t.email =  'tanya.a.marwah@gmail.com'
                                                        )t
                                                        left join
                                                        (
                                                            select t.email, t.nextbday, t.occasion as days_to_occasion
                                                            from
                                                            (
                                                                select t.email, to_char(t.next_bday,'MON-DD') as nextbday,
                                                                (case 
                                                                when t.dif >= 350 then cast(t.dif-365 as text)
                                                                else cast(t.dif as text)
                                                                end) as occasion
                                                                from
                                                                (
                                                                    select t.email, t.bday, t.next_bday, t.next_bday-current_date as dif
                                                                    from
                                                                    (
                                                                        select t.email, t.bday, t.next_bday, t.next_bday-30 as startdt, t.next_bday+15 as enddt
                                                                        from
                                                                        (
                                                                            select t.email as email, t.bday,
                                                                            cast(date(t.bday) + ((extract(year from age(date(current_date), date(t.bday))) + 1) * interval '1' year) as date) as next_bday
                                                                            from 
                                                                            (
                                                                                select t.email, t.bday
                                                                                from
                                                                                (
                                                                                    select t.email, t.bday, 
                                                                                    dense_rank() over(partition by t.email order by t.dt desc) as rank,
                                                                                    row_number() over(partition by t.email order by t.dt desc) as rown
                                                                                    from
                                                                                    (
                                                                                        select lower(customer_email) as email, birth_date as bday, (_epoch at time zone 'UTC+5:30') as dt
                                                                                        from gmg_92ca38ca.bday_anniv_automate_dump
                                                                                        where birth_date <> 'NULL' and birth_date is not null and birth_date <> '0000-00-00' 
                                                                                        and length(birth_date) = '10' -- and birth_month in ('9','10','11')
                                                                                        -- and customer_email = 'ashutosh.sharma06@gmail.com'
                                                                                        union all
                                                                                        select lower(email) as email, birth_date as bday, (_epoch at time zone 'UTC+5:30') as dt
                                                                                        from gmg_92ca38ca.domo_occasion_data
                                                                                        where birth_date <> 'NULL' and birth_date is not null and birth_date <> '0000-00-00' 
                                                                                        and length(birth_date) = '10' -- and birth_month in ('9','10','11')
                                                                                        -- and email = 'ashutosh.sharma06@gmail.com'
                                                                                    )t
                                                                                )t
                                                                                where t.rank = 1 and t.rown = 1				
                                                                            )t
                                                                        )t
                                                                    )t
                                                                )t
                                                            )t
                                                            where cast(t.occasion as numeric) >= 0 and cast(t.occasion as numeric) <= 30 -- and t.email = 'tanya.a.marwah@gmail.com'
                                                        )b on b.email = t.email
                                                        left join
                                                        (
                                                            select t.email, t.nextanniv, t.occasion as days_to_occasion
                                                            from
                                                            (
                                                                select t.email, to_char(t.next_anniv,'MON-DD') as nextanniv,
                                                                (case 
                                                                when t.dif >= 350 then cast(t.dif-365 as text)
                                                                else cast(t.dif as text)
                                                                end) as occasion
                                                                from
                                                                (
                                                                    select t.email, t.anniv, t.next_anniv, t.next_anniv-current_date as dif
                                                                    from
                                                                    (
                                                                        select t.email, t.anniv, t.next_anniv, t.next_anniv-30 as startdt, t.next_anniv+15 as enddt
                                                                        from
                                                                        (
                                                                            select t.email as email, t.anniv,
                                                                            cast(date(t.anniv) + ((extract(year from age(date(current_date), date(t.anniv))) + 1) * interval '1' year) as date) as next_anniv
                                                                            from 
                                                                            (
                                                                                select t.email, t.anniv
                                                                                from
                                                                                (
                                                                                    select t.email, t.anniv, 
                                                                                    dense_rank() over(partition by t.email order by t.dt desc) as rank,
                                                                                    row_number() over(partition by t.email order by t.dt desc) as rown
                                                                                    from
                                                                                    (
                                                                                        select lower(customer_email) as email, anniversary_date as anniv, (_epoch at time zone 'UTC+5:30') as dt
                                                                                        from gmg_92ca38ca.bday_anniv_automate_dump
                                                                                        where anniversary_date <> 'NULL' and anniversary_date is not null and anniversary_date <> '0000-00-00' 
                                                                                        and length(anniversary_date) = '10' -- and birth_month in ('9','10','11')
                                                                                        -- and customer_email = 'ashutosh.sharma06@gmail.com'
                                                                                        union all
                                                                                        select lower(email) as email, anniversary_date as anniv, (_epoch at time zone 'UTC+5:30') as dt
                                                                                        from gmg_92ca38ca.domo_occasion_data
                                                                                        where anniversary_date <> 'NULL' and anniversary_date is not null and anniversary_date <> '0000-00-00' 
                                                                                        and length(anniversary_date) = '10' -- and birth_month in ('9','10','11')
                                                                                        -- and email = 'ashutosh.sharma06@gmail.com'
                                                                                    )t
                                                                                )t
                                                                                where t.rank = 1 and t.rown = 1				
                                                                            )t
                                                                        )t
                                                                    )t
                                                                )t
                                                            )t
                                                            where cast(t.occasion as numeric) >= 0 and cast(t.occasion as numeric) <= 30 -- and t.email = 'tanya.a.marwah@gmail.com'
                                                        )a on a.email = t.email 
                                                        left join
                                                        (
                                                            select t.email, t.nextspbday, t.occasion as days_to_occasion
                                                            from
                                                            (
                                                                select t.email, to_char(t.next_spbday,'MON-DD') as nextspbday,
                                                                (case 
                                                                when t.dif >= 350 then cast(t.dif-365 as text)
                                                                else cast(t.dif as text)
                                                                end) as occasion
                                                                from
                                                                (
                                                                    select t.email, t.spbday, t.next_spbday, t.next_spbday-current_date as dif
                                                                    from
                                                                    (
                                                                        select t.email, t.spbday, t.next_spbday, t.next_spbday-30 as startdt, t.next_spbday+15 as enddt
                                                                        from
                                                                        (
                                                                            select t.email as email, t.spbday,
                                                                            cast(date(t.spbday) + ((extract(year from age(date(current_date), date(t.spbday))) + 1) * interval '1' year) as date) as next_spbday
                                                                            from 
                                                                            (
                                                                                select t.email, t.spbday
                                                                                from
                                                                                (
                                                                                    select t.email, t.spbday, 
                                                                                    dense_rank() over(partition by t.email order by t.dt desc) as rank,
                                                                                    row_number() over(partition by t.email order by t.dt desc) as rown
                                                                                    from
                                                                                    (
                                                                                        select lower(customer_email) as email, spouse_birthdate as spbday, (_epoch at time zone 'UTC+5:30') as dt
                                                                                        from gmg_92ca38ca.bday_anniv_automate_dump
                                                                                        where spouse_birthdate <> 'NULL' and spouse_birthdate is not null and spouse_birthdate <> '0000-00-00' 
                                                                                        and length(spouse_birthdate) = '10' -- and birth_month in ('9','10','11')
                                                                                        -- and customer_email = 'ashutosh.sharma06@gmail.com'
                                                                                        union all
                                                                                        select lower(email) as email, spouse_birthday as spbday, (_epoch at time zone 'UTC+5:30') as dt
                                                                                        from gmg_92ca38ca.domo_occasion_data
                                                                                        where spouse_birthday <> 'NULL' and spouse_birthday is not null and spouse_birthday <> '0000-00-00' 
                                                                                        and length(spouse_birthday) = '10' -- and birth_month in ('9','10','11')
                                                                                        -- and email = 'ashutosh.sharma06@gmail.com'
                                                                                    )t
                                                                                )t
                                                                                where t.rank = 1 and t.rown = 1				
                                                                            )t
                                                                        )t
                                                                    )t
                                                                )t
                                                            )t
                                                            where cast(t.occasion as numeric) >= 0 and cast(t.occasion as numeric) <= 30 -- and t.email = 'tanya.a.marwah@gmail.com'
                                                        )p on p.email = t.email 
                                                        left join
                                                        (
                                                            select t.email, t.xcl_balance
                                                            from
                                                            (
                                                                select email, xcl_balance, dense_rank() over(partition by email order by _epoch desc) as rank
                                                                from gmg_92ca38ca.xclusive_daily_dump 
                                                                where cast(xcl_min_expirydate as date) >= current_date
                                                                order by _epoch desc
                                                            )t
                                                            where t.rank = 1 and t.xcl_balance >= 1000
                                                        )x on x.email = t.email
                                                        left join
                                                        (
                                                            select t.email, t.ptype
                                                            from
                                                            (
                                                                select t.email, t.ptype, t.skucnt,
                                                                dense_rank() over(partition by t.email order by t.skucnt desc) as rank,
                                                                row_number() over(partition by t.email order by t.skucnt desc) as rown
                                                                from
                                                                (
                                                                    select lower(d.email) as email, pdp_producttype as ptype, count(distinct pdp_sku) as skucnt
                                                                    from gmg_92ca38ca.pdp_viewproduct p
                                                                    join gmg_92ca38ca.ds_data_9505 d on d.vis = p._visid
                                                                    where date(_epoch at time zone 'UTC+5:30') between date(current_date)-21 and date(current_date)-1
                                                                    group by 1,2
                                                                )t
                                                            )t
                                                            join 
                                                            (
                                                                select distinct(email) from gmg_92ca38ca.ds_data_10215 --repeat_upto to be changed start of every month
                                                            )r on lower(r.email) = lower(t.email)
                                                            where t.rank = 1 and t.rown = 1 
                                                        )pd on pd.email = t.email
                                                    )t
                                                )t
                                                left join gmg_92ca38ca.ds_data_9588 p on p.pincode = t.pincode 
                                            )t
                                            join gmg_92ca38ca.ml_prospect_repeat m on m.email = t.email and date(m._epoch at time zone 'UTC+5:30') = current_date



                                            -- where t.email = 'tanya.a.marwah@gmail.com'

                                            -- select * from gmg_92ca38ca.xclusive_daily_dump limit 10

                                            /*
                                            select customer_email, birth_date, anniversary_date, spouse_birthdate
                                            from gmg_92ca38ca.bday_anniv_automate_dump limit 10

                                            select customer_email as email, birth_date as bday, anniversary_date, spouse_birthday
                                            from gmg_92ca38ca.bday_anniv_automate_dump limit 10
                                            */

                                            """, con=connection)

    return df_repeat