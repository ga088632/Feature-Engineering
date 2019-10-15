/*** Contract Grouping ***/

/**Base**/
--drop table Contract_Data;
Create volatile table Contract_Data as
(
Sel a.subs_id
,prd_dt
,Plan_Cntrct_Start_Dt
, MPP_Cntrct_Start_Dt
, Plan_Cntrct_Trm
, MPP_Cntrct_End_Dt
, Plan_Cntrct_End_Dt
, MPP_Cntrct_Trm
,Value_band_1

from aupr_bus_view.fact_ppltn_snap a

left join aupr_bus_view.dim_propsn_attrs b 
on a.prim_prod_key = b.prod_key

left join aupr_bus_view.dim_cust c 
on a.cust_id = c.cust_id 
and a.prd_dt between c.dim_start_dt and c.dim_end_dt

left join aupr_bus_view.dim_subs d
on a.subs_id = d.subs_id 
and a.prd_dt between d.dim_start_dt and d.dim_end_dt

where prd_dt ='2019-08-24' 
and Coalesce(b.acct_type_cd,'Postpay') = 'Postpay' 
and Coalesce(b.plan_type_cd,'handset') = 'handset' 
and ppltn_type_id =1 
and prd_type_cd ='DAY' 
and c.cust_type_cd ='Consumer'
)with data 
primary index(subs_id)
on commit preserve rows;


--drop table Contract_group;
Create  volatile table Contract_group as
(

sel a.*
,All_Cntrcat_Start_Dt
,All_Cntrcat_End_Dt
, (All_Cntrcat_End_Dt-prd_dt) as No_of_Days_bef_Contract_end
 ,(prd_dt-All_Cntrcat_End_Dt) as No_of_Days_aft_Contract_End
 , (prd_dt-All_Cntrcat_Start_Dt) as No_of_Days_from_Contract_Start
,Case 
when Value_Band_1 like '%SIM%Only%' and Plan_Cntrct_Start_Dt is null and MPP_Cntrct_Start_Dt is null then 'No Contract SIMO'
when Value_Band_1 not like '%SIM%Only%' and Plan_Cntrct_Start_Dt is null and MPP_Cntrct_Start_Dt is null then 'No Contract HS'
when Value_Band_1 like '%SIM%Only%' and (prd_dt-All_Cntrcat_Start_Dt) <= 92  then 'Early Life SIMO'
when Value_Band_1 not like '%SIM%Only%' and (prd_dt-All_Cntrcat_Start_Dt) <= 92  then 'Early Life HS'
when Value_Band_1 like '%SIM%Only%' and  (All_Cntrcat_End_Dt-prd_dt) between 0 and 122 then 'Loyalty SIMO'
when   (All_Cntrcat_End_Dt-prd_dt)  between 0 and 61 then 'Loyalty HS'
when Value_Band_1 like '%SIM%Only%' and  (prd_dt-All_Cntrcat_End_Dt) between 0 and 92 then 'OOC SIMO'
when Value_Band_1 not like '%SIM%Only%' and  (prd_dt-All_Cntrcat_End_Dt) between 0 and 92 then 'OOC HS'
when  Value_Band_1 like '%SIM%Only%' and (prd_dt-All_Cntrcat_End_Dt) >92 then 'LOOC SIMO'
when  Value_Band_1 not like '%SIM%Only%' and (prd_dt-All_Cntrcat_End_Dt) >92 then 'LOOC HS'
when Value_Band_1 like '%SIM%Only%' and (prd_dt-All_Cntrcat_Start_Dt) between  122 and 244  then 'Mid Life SIMO'
when Value_Band_1 not like '%SIM%Only%' and (prd_dt-All_Cntrcat_Start_Dt) between  122 and 244  then 'Mid Life HS'
when Value_Band_1 like '%SIM%Only%' then 'In-Life SIMO'
Else 'In-Life HS'
end as Contract_Group
from Contract_Data a
inner join 
(
Sel subs_id
,Max(Coalesce(Plan_Cntrct_Start_Dt,MPP_Cntrct_Start_Dt)) as  All_Cntrcat_Start_Dt
,Max(Coalesce(Plan_Cntrct_end_Dt,MPP_Cntrct_End_Dt)) as  All_Cntrcat_End_Dt
from Contract_Data
group by 1
)b
on a.subs_id=b.subs_id
)with data 
primary index(subs_id)
on commit preserve rows;


drop table BI_ANALYTICS_TEMP_DB.SG_Contract_Group;
Create table BI_ANALYTICS_TEMP_DB.SG_Contract_Group as
(sel * from Contract_Group
)with data 
primary index(subs_id);




Create volatile table Campaign_Data as
(
sel csic.subs_id,cic.campaign_cd,contact_dt,cic.campaign_nm
from aupr_cdm_view.CI_SUBS_CONTACT_HISTORY  CSIC
INNER JOIN aupr_bus_view.dim_subs ds
ON CSIC.subs_id = ds.subs_id
	AND CSIC.contact_dt BETWEEN ds.dim_start_dt AND ds.dim_end_dt
	AND ds.subs_id > 0
INNER JOIN aupr_cdm_view.CI_CELL_PACKAGE CSP
     ON CSIC.CELL_PACKAGE_SK = CSP.CELL_PACKAGE_SK     
 INNER JOIN aupr_cdm_view.ci_campaign cic
     ON CSP.CAMPAIGN_SK = CIC.CAMPAIGN_SK     
where  CAST(CONTACT_DTTM AS DATE ) between  '2019-05-24' and '2019-08-24'
	AND  CSIC.CONTACT_HISTORY_STATUS_CD <> '_31'
	AND  ds.acct_type_cd IN ('Postpay')        
   AND cic.campaign_cd in (
        'CAMP612042',
        'CAMP610975',
        'CAMP612838',
        'CAMP611914',
        'CAMP612716',
        'CAMP613096',
        'CAMP613092',
        'CAMP612232',
        'CAMP613134',
        'CAMP611509',
        'CAMP612959',
        'CAMP613095',
        'CAMP613111',
        'CAMP611139',
        'CAMP613221',
        'CAMP612234',
        'CAMP613216',
        'CAMP612580',
        'CAMP611829',
        'CAMP611855',
        'CAMP612016',
        'CAMP611759',
        'CAMP612416',
        'CAMP611512',
        'CAMP611511',
        'CAMP611510',
        'CAMP612987',
        'CAMP612847',
        'CAMP612837',
        'CAMP612253',
        'CAMP611513',
        'CAMP612738',
        'CAMP612253',
        'CAMP612251',
        'CAMP612105',
        'CAMP611541',
        'CAMP612234',
        'CAMP612106',
        'CAMP612252',
        'CAMP612300',
        'CAMP611502',
        'CAMP612276',
        'CAMP612253',
        'CAMP612229',
        'CAMP612103',
        'CAMP611975',
        'CAMP611986',
        'CAMP610427',
        'CAMP610455',
        'CAMP611990',
        'CAMP611984',
        'CAMP611989',
        'CAMP611977',
        'CAMP611832',
        'CAMP611944',
        'CAMP611988',
        'CAMP611692',
        'CAMP611973',
        'CAMP611695',
        'CAMP610449',
        'CAMP610962',
        'CAMP611838',
        'CAMP611828',
        'CAMP611837',
        'CAMP611797',
        'CAMP611834',
        'CAMP611115',
        'CAMP611174',
        'CAMP611162',
        'CAMP611171',
        'CAMP611118',
        'CAMP611462',
        'CAMP611667',
        'CAMP611164',
        'CAMP610860',
        'CAMP611539',
        'CAMP611425',
        'CAMP611451',
        'CAMP611424',
        'CAMP611144',
        'CAMP611187',
        'CAMP611165',
        'CAMP610870',
        'CAMP507811',
        'CAMP610803',
        'CAMP610873',
        'CAMP610964',
        'CAMP610988',
        'CAMP610985',
        'CAMP611011',
        'CAMP610970',
        'CAMP613291',
        'CAMP613292', 
        'CAMP613249',
        'CAMP613319', 
        'CAMP612434',
        'CAMP613345',
        'CAMP613352', 
        'CAMP613353',
        'CAMP613397',
     	'CAMP613161',
     	'CAMP613249',
        'CAMP612107',
        'CAMP613454',
        'CAMP611640',
        'CAMP613352',
        'CAMP612959',
        'CAMP611509',
        'CAMP502641',
        'CAMP611915',
        'CAMP613509',
        'CAMP613504',
        'CAMP613511',
        'CAMP613512',
        'CAMP613508',
        'CAMP613545',
        'CAMP613514',
        'CAMP613540'
) 
)with data 
primary index(subs_id)
on commit preserve rows;

drop table Campaign_Contract_Group;
Create volatile table Campaign_Contract_Group as
(
Select 
distinct 
a.Subs_id,
a.prd_dt,
Contract_Group, 
case when b.subs_id is not null then 'Y' else 'N' end as Contact_Commercial,
b.Numbers as Noof_Com_Campaigncodes,
campaign_nm_1,
campaign_nm_2,
campaign_nm_3,
campaign_nm_4,
campaign_nm_5,
campaign_nm_6,
campaign_nm_7,
campaign_nm_8,
campaign_nm_9,
campaign_nm_10,
campaign_nm_11,
campaign_nm_12,
campaign_nm_13,
campaign_nm_14,
campaign_nm_15,
campaign_nm_16,
campaign_nm_17,
campaign_nm_18,
campaign_nm_19,
campaign_nm_20,
campaign_nm_21,
campaign_nm_22,
campaign_nm_23

from Contract_Group a

left join 
(
select 
subs_id,
count(campaign_nm) as Numbers
 from Campaign_Data a
group by 1
)b
on a.subs_id=b.subs_id
left join
(
select 
subs_id,
campaign_nm as campaign_nm_1
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=1
)c
on a.subs_id=c.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_2
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=2
)d
on a.subs_id=d.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_3
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=3
)e
on a.subs_id=e.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_4
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=4
)f
on a.subs_id=f.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_5
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=5
)g
on a.subs_id=g.subs_id


left join
(
select 
subs_id,
campaign_nm as campaign_nm_6
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=6
)h
on a.subs_id=h.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_7
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=7
)i
on a.subs_id=i.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_8
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=8
)j
on a.subs_id=j.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_9
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=9
)k
on a.subs_id=k.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_10
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=10
)l
on a.subs_id=l.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_11
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=11
)m
on a.subs_id=m.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_12
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=12
)n
on a.subs_id=n.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_13
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=13
)o
on a.subs_id=o.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_14
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=14
)p
on a.subs_id=p.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_15
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=15
)q
on a.subs_id=q.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_16
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=16
)r
on a.subs_id=r.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_17
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=17
)s
on a.subs_id=s.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_18
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=18
)t
on a.subs_id=t.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_19
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=19
)u
on a.subs_id=u.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_20
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=20
)v
on a.subs_id=v.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_21
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=21
)w
on a.subs_id=w.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_22
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=22
)x
on a.subs_id=x.subs_id

left join
(
select 
subs_id,
campaign_nm as campaign_nm_23
	
from Campaign_Data a
qualify(rank() over (partition by subs_id order by contact_dt desc,campaign_nm desc))=23
)y
on a.subs_id=y.subs_id


)with data 
primary index(subs_id)
on commit preserve rows;

sel * from bi_analytics_temp_db.LR_SPV_2018_V2 

CREATE VOLATILE TABLE SPV_Bucket as
(SELECT DISTINCT
                a.subs_id
                ,CASE 
                WHEN    a.SPV <= 0 THEN 1
                WHEN    a.SPV > 0 and a.SPV <= 200 THEN 2
                WHEN     a.SPV > 200 and a.SPV <= 500 THEN 3
                WHEN    a.SPV > 500 THEN 4
                END as SPV_Bucket
  
                from bi_analytics_temp_db.LR_SPV_2018_V2 a
                
                left join aupr_bus_view.cmpst_subs_drct_mrgn b
                on a.subs_id = b.subs_id 
                and a.prd_dt = b.prd_dt
                
                where b.prd_type_cd = 'mthly'
                and b.cust_type_cd = 'Consumer'
                and b.acct_Type_cd = 'Postpay'
                and b.plan_type_cd = 'Handset'
                and a.prd_dt = '2019-08-31'
)with data
primary index(subs_Id)
on commit preserve rows;


CREATE VOLATILE TABLE MARGIN_PERCENTILE AS
(SEL
        csdm.subs_id,
        Mthly_Margin,
        round( floor(10* PERCENT_RANK() OVER (ORDER BY Mthly_Margin desc ) ),0) AS Margin_Percentile    
        
        from 
        (SEL
                subs_id,
                Subs_DrctMrgn_ExclHRR_ExGST_A - ((INB_Data_Dom_MBs + OOB_Data_Dom_Mbs) / 1024) as Mthly_Margin
                -- Add Outbound Revenue + Interconnect Revenue
            --Less Interconnect Cost, National Roaming Cost, International Roaming Cost, Cost to Carry @ $1 Per GB (Domestic Data MBs / 1024)
                              
   
                from aupr_bus_view.cmpst_subs_Drct_mrgn a
                
                where a.prd_type_cd = 'mthly'
                and a.cust_type_cd = 'Consumer'
                and a.acct_Type_cd = 'Postpay'
                and a.plan_type_cd = 'Handset'
                and a.prd_dt = '2019-08-31'
        )csdm    
        
)WITH DATA
PRIMARY INDEX(SUBS_ID)
ON COMMIT PRESERVE ROWS;


CREATE VOLATILE TABLE MARGIN_PERCENTILE_3Mths AS
(SEL
        csdm.subs_id,
        Mthly_Margin,
        round( floor(10* PERCENT_RANK() OVER (ORDER BY Mthly_Margin desc ) ),0) AS Margin_Percentile
        
        
        from 
        (SEL
                subs_id,
                Subs_DrctMrgn_ExclHRR_ExGST_A - ((INB_Data_Dom_MBs + OOB_Data_Dom_Mbs) / 1024) as Mthly_Margin
                    -- Add Outbound Revenue + Interconnect Revenue
            --Less Interconnect Cost, National Roaming Cost, International Roaming Cost, Cost to Carry @ $1 Per GB (Domestic Data MBs / 1024)
                              
   
                from aupr_bus_view.cmpst_subs_Drct_mrgn a
                
                where a.prd_type_cd = 'Mvg_3Mth_Mthly_Avg'
                and a.cust_type_cd = 'Consumer'
                and a.acct_Type_cd = 'Postpay'
                and a.plan_type_cd = 'Handset'
                and a.prd_dt = '2019-08-31'
        )csdm    
        
)WITH DATA
PRIMARY INDEX(SUBS_ID)
ON COMMIT PRESERVE ROWS;            

drop table Churn_data;
Create volatile table Churn_data as
(
sel a.*,
Contract_Group, 
Contact_Commercial,
Noof_Com_Campaigncodes,
campaign_nm_1,
campaign_nm_2,
campaign_nm_3,
campaign_nm_4,
campaign_nm_5,
campaign_nm_6,
campaign_nm_7,
campaign_nm_8,
campaign_nm_9,
campaign_nm_10,
campaign_nm_11,
campaign_nm_12,
campaign_nm_13,
campaign_nm_14,
campaign_nm_15,
campaign_nm_16,
campaign_nm_17,
campaign_nm_18,
campaign_nm_19,
campaign_nm_20,
campaign_nm_21,
campaign_nm_22,
campaign_nm_23,
Margin_Percentile,
Mthly_Margin,
case when up.subs_id is not null then 1 else 0 end as Upgrade_flag,
b.prd_dt
/*Tot_Wgt_Avg_prd_util,
avg_Tot_Wgt_Avg_prd_uti,
Cell_SA2_name,                                   
Cell_SA3_name ,                                     
Cell_SA4_name     */

from    bi_analytics_temp_db.GA_To_Train_Subset  a

inner join  Campaign_Contract_Group b
 on a.ID_subs_id=b.subs_id
 
left join  MARGIN_PERCENTILE_3Mths c
 on a.ID_subs_id=c.subs_id
 
 left join 
 (select *  

from AUPR_BUS_VIEW.CMPST_MVMNT_EVENT  
where Prd_dt between '2019-08-24'  and '2019-09-23'
and Acct_type_cd='Postpay'
and Plan_type_cd='Handset'
and Cust_type_cd='Consumer'
and meas_type_cd = 'RTNTN')up
on a.ID_subs_id=up.subs_id
 
/* left join BI_ANALYTICS_TEMP_DB.SG_Cell_hh_Wgt_Avg_prd_util_Final d
 on a.subs_id=d.subs_id;*/
 )with data 
 primary index(ID_subs_id)
 on commit preserve rows;
 
 /** New Features**/
 
 /** IKEA and Westfield site accessing services */

/** Extract the Base population as of 24th August 2019**/
drop table All_Subs;
Create volatile table All_Subs as
(
sel subs_id,
ACCT_TYPE_CD  ,                
PRIM_PROD_KEY,                 
DLR_CD  ,                                        
GOLDEN_PHYS_ADDR_ID           

FROM aupr_bus_view.Fact_Ppltn_Snap c
    where 
    (prd_dt = '2019-08-24')
	and prd_type_cd='DAY' 
    and ppltn_type_id=1
    and brnd_cd='Vodafone' 
    ---and Acct_type_Cd = 'Postpay'
    and subs_id>0
)with data 
primary index(subs_id)
on commit preserve rows;

/** Get the 3 months Packet network data from June19 to Aug19**/
Create volatile table Packet_data_3months as
(
sel a.*,extract(hour from Ntwk_First_Dttm)  as Ntwk_First_DtHr, cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') as Ntwk_First_Dt
from AUPR_BUS_VIEW.Fact_Pckt_Ntwk_Usg_Acty a
where subs_id in (sel subs_id from   All_Subs)
and cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') between '2019-06-01' and '2019-06-10'
)with data 
primary index(subs_id,Ntwk_First_Dttm)
on commit preserve rows;

INSERT INTO Packet_data_3months
sel a.*,extract(hour from Ntwk_First_Dttm)  as Ntwk_First_DtHr, cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') as Ntwk_First_Dt
from AUPR_BUS_VIEW.Fact_Pckt_Ntwk_Usg_Acty a
where subs_id in (sel subs_id from   All_Subs)
and cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') between '2019-06-22' and '2019-06-31';


INSERT INTO  Packet_data_3months 
sel a.*,extract(hour from Ntwk_First_Dttm)  as Ntwk_First_DtHr, cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') as Ntwk_First_Dt
from AUPR_BUS_VIEW.Fact_Pckt_Ntwk_Usg_Acty a
where subs_id in (sel subs_id from   All_Subs)
and cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') between '2019-07-01' and '2019-07-31';


INSERT INTO  Packet_data_3months 
sel a.*,extract(hour from Ntwk_First_Dttm)  as Ntwk_First_DtHr, cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') as Ntwk_First_Dt
from AUPR_BUS_VIEW.Fact_Pckt_Ntwk_Usg_Acty a
where subs_id in (sel subs_id from   All_Subs)
and cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') between '2019-08-01' and '2019-08-20';

INSERT INTO  Packet_data_3months 
sel a.*,extract(hour from Ntwk_First_Dttm)  as Ntwk_First_DtHr, cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') as Ntwk_First_Dt
from AUPR_BUS_VIEW.Fact_Pckt_Ntwk_Usg_Acty a
where subs_id in (sel subs_id from   All_Subs)
and cast(Ntwk_First_Dttm as date) (format 'YYYY-MM-DD') between '2019-08-21' and '2019-08-31';


drop table   BI_ANALYTICS_TEMP_DB.SG_Packet_data;
Create  table BI_ANALYTICS_TEMP_DB.SG_Packet_data as
(
sel 
Subs_Id                                                                
,Frst_Cell_Id   
,Ntwk_First_Dt       
,sum(Pckt_Ntwk_Up_DOwn_Scnds)  as Tot_Pckt_Ntwk_Up_DOwn_Scnds     
,sum(Pckt_Ntwk_Upld_Vol_Kbs)   as Tot_Pckt_Ntwk_Upld_Vol_Kbs    
,sum(Pckt_Ntwk_Dwnld_Vol_Kbs)  as Tot_Pckt_Ntwk_Dwnld_Vol_Kbs     
,sum(Pckt_Ntwk_Event_Cnt) as Tot_Pckt_Ntwk_Event_Cnt

from Packet_data_3months
group by 1,2,3
)with data 
primary index(subs_id,Ntwk_First_Dt,Frst_Cell_Id );



drop table TEMP_DB.SG_Packet_data_Flag ;
Create table  TEMP_DB.SG_Packet_data_Flag as
(
sel a.*,
b.Cell_name as ShoppingCenter_Cellname, 
case when b.Cell_name is not null then 1 else 0 end as In_ShoppingCenter_Flag,
c.Cell_name as IKEA_Cellname, 
case when c.Cell_name is not null then 1 else 0 end as In_IKEA_Flag

from  BI_ANALYTICS_TEMP_DB.SG_Packet_data a

left join 
(
sel * from  AUPR_BUS_VIEW.DIM_CELL_LOC_INSTC where cell_name is  not null
and Cell_Loc_Instc_End_dt='9999-12-31' and Cell_name in (sel cellname from BI_ANALYTICS_TEMP_DB.SG_Shooping_center)
)b
on a.Frst_Cell_Id=b.Cell_id

left join 
(
sel * from   AUPR_BUS_VIEW.DIM_CELL_LOC_INSTC where cell_name is  not null
and Cell_LOc_Instc_End_dt='9999-12-31' and Cell_name like any ('%Ikea%')
)c
on a.Frst_Cell_Id=c.Cell_id

)with data 
primary index(subs_id,Ntwk_First_DtHr,Frst_Cell_Id );



Create volatile table Top_Usage_Cluster as
(
sel subs_id,
Cluster_Name,
sum(Tot_Pckt_Ntwk_Dwnld_Vol_Kbs)/(1024) as sum_Dwnld_Vol_Kbs

from BI_ANALYTICS_TEMP_DB.SG_Packet_data a

inner join (sel * from  AUPR_BUS_VIEW.DIM_CELL_LOC_INSTC where cell_name is  not null
and Cell_Loc_Instc_End_dt='9999-12-31') c
on a.Frst_Cell_Id=c.Cell_id

inner join BI_ANALYTICS_TEMP_DB.SG_Cell_Segment b
on c.Cell_name=b.Cellname

group by 1,2
qualify(rank() over (partition by subs_id order by sum_Dwnld_Vol_Kbs desc))=1

)with data 
primary index(subs_id,Cluster_Name )
on commit preserve rows;

Create volatile table Top2_Usage_Cluster as
(
sel subs_id,
Cluster_Name,
sum(Tot_Pckt_Ntwk_Dwnld_Vol_Kbs)/(1024) as sum_Dwnld_Vol_Kbs

from BI_ANALYTICS_TEMP_DB.SG_Packet_data a

inner join (sel * from  AUPR_BUS_VIEW.DIM_CELL_LOC_INSTC where cell_name is  not null
and Cell_Loc_Instc_End_dt='9999-12-31') c
on a.Frst_Cell_Id=c.Cell_id

inner join BI_ANALYTICS_TEMP_DB.SG_Cell_Segment b
on c.Cell_name=b.Cellname

group by 1,2
qualify(rank() over (partition by subs_id order by sum_Dwnld_Vol_Kbs desc))=2

)with data 
primary index(subs_id,Cluster_Name )
on commit preserve rows;

Create volatile table Top3_Usage_Cluster as
(
sel subs_id,
Cluster_Name,
sum(Tot_Pckt_Ntwk_Dwnld_Vol_Kbs)/(1024) as sum_Dwnld_Vol_Kbs

from BI_ANALYTICS_TEMP_DB.SG_Packet_data a

inner join (sel * from  AUPR_BUS_VIEW.DIM_CELL_LOC_INSTC where cell_name is  not null
and Cell_Loc_Instc_End_dt='9999-12-31') c
on a.Frst_Cell_Id=c.Cell_id

inner join BI_ANALYTICS_TEMP_DB.SG_Cell_Segment b
on c.Cell_name=b.Cellname

group by 1,2
qualify(rank() over (partition by subs_id order by sum_Dwnld_Vol_Kbs desc))=3

)with data 
primary index(subs_id,Cluster_Name )
on commit preserve rows;


Create volatile table Commuter_Metro_Usage_Cluster as
(
sel subs_id,
Cluster_Name,
sum(Tot_Pckt_Ntwk_Dwnld_Vol_Kbs)/(1024) as sum_Dwnld_Vol_Kbs

from BI_ANALYTICS_TEMP_DB.SG_Packet_data a

inner join (sel * from  AUPR_BUS_VIEW.DIM_CELL_LOC_INSTC where cell_name is  not null
and Cell_Loc_Instc_End_dt='9999-12-31') c
on a.Frst_Cell_Id=c.Cell_id

inner join BI_ANALYTICS_TEMP_DB.SG_Cell_Segment b
on c.Cell_name=b.Cellname
where Cluster_Name='Commuter Metro'

group by 1,2
having sum_Dwnld_Vol_Kbs>150

)with data 
primary index(subs_id,Cluster_Name )
on commit preserve rows;


drop table BI_ANALYTICS_TEMP_DB.SG_Top3Cluster_Commuter_Data ;
Create table  BI_ANALYTICS_TEMP_DB.SG_Top3Cluster_Commuter_Data as
(
sel a.*,
b.Cluster_Name as Top2_Cluster_Name,
b.sum_Dwnld_Vol_Kbs as Top2_sum_Dwnld_Vol_Kbs,
c.Cluster_Name as Top3_Cluster_Name,
c.sum_Dwnld_Vol_Kbs as Top3_sum_Dwnld_Vol_Kbs,
d.Cluster_Name as Commuter_Cluster_Name,
d.sum_Dwnld_Vol_Kbs as Commuter_sum_Dwnld_Vol_Kbs

from  Top_Usage_Cluster a

left join Top2_Usage_Cluster b
on a.subs_id=b.subs_id

left join Top3_Usage_Cluster c
on a.subs_id=c.subs_id

left join Commuter_Metro_Usage_Cluster  d
on a.subs_id=d.subs_id

qualify(rank() over (partition by a.subs_id order by a.Cluster_Name                  
,a.sum_Dwnld_Vol_Kbs             
,Top2_Cluster_Name             
,Top2_sum_Dwnld_Vol_Kbs        
,Top3_Cluster_Name             
,Top3_sum_Dwnld_Vol_Kbs        
,Commuter_Cluster_Name         
,Commuter_sum_Dwnld_Vol_Kbs  ))=1  


)with data 
primary index(subs_id);

sel * from BI_ANALYTICS_TEMP_DB.SG_Top3Cluster_Commuter_Data
/* Night Owls */

Create volatile table Data_GSMSI_Key as
(
sel distinct
    GSMI_CT.GSMSI_Key,
    UC_CT.Usg_Class_Type_Cd Usg_Class_Type_Cd_c,
    UC_CT.Usg_Class_Cd Usg_Class_Cd_c,
    UC_CT2.Usg_Class_Type_Cd Usg_Class_Type_Cd_f,
    UC_CT2.Usg_Class_Cd Usg_Class_Cd_f
from
     aupr_model_view.GSMSI AS GSMI_CT
     inner join aupr_model_view.GSMSI_USG_CLASS_ASSN as GUCA_CT on GSMI_CT.GSMSI_Key = GUCA_CT.GSMSI_Key and GUCA_CT.GSMSI_Usg_Class_End_dt = '9999-12-31'
     inner join aupr_model_view.USG_CLASS as UC_CT on GUCA_CT.Usg_Class_Key = UC_CT.Usg_Class_Key and UC_CT.Usg_Class_Type_Cd = 'CALL_TYPE'
     inner join aupr_model_view.GSMSI_USG_CLASS_ASSN as GUCA_CT2
         on GSMI_CT.GSMSI_Key = GUCA_CT2.GSMSI_Key and GUCA_CT2.GSMSI_Usg_Class_End_dt = '9999-12-31'
     inner join aupr_model_view.USG_CLASS as UC_CT2 on GUCA_CT2.Usg_Class_Key = UC_CT2.Usg_Class_Key and UC_CT2.Usg_Class_Type_Cd = 'FINANCE'
where GUCA_CT.GSMSI_Usg_Class_End_Dt = '9999-12-31'
and   Usg_Class_Cd_f like '%DATA%'
)with data
primary index(GSMSI_Key)
on commit preserve rows;

--drop table Aug19_Postpay_Hourly_Usage;
Create volatile table Aug19_Postpay_Hourly_Usage as
(
Sel     
Acct_Id          ,             
Accs_Num    ,      
subs_id,       
Cal_Dt         ,        
Fin_Rcgntn_Dt      ,  
Usg_Dirctn_Key    ,   
Prim_Prod_Key   ,              
Prod_Key       ,                       
uai.GSMSI_Key     ,     
Usg_Class_Cd_c,
Usg_Class_Cd_f    ,             
case when Tm_Of_Day_Tm  between '00:00:00' and '00:59:59'  then 0
when Tm_Of_Day_Tm  between  '01:00:00' and '01:59:59' then 1
when Tm_Of_Day_Tm  between  '02:00:00' and '02:59:59'  then 2
when Tm_Of_Day_Tm  between  '03:00:00' and '03:59:59'  then 3
when Tm_Of_Day_Tm  between  '04:00:00' and '04:59:59'  then 4
when Tm_Of_Day_Tm  between  '05:00:00' and '05:59:59'  then 5
when Tm_Of_Day_Tm  between  '06:00:00' and '06:59:59'  then 6
when Tm_Of_Day_Tm  between  '07:00:00' and '07:59:59'  then 7
when Tm_Of_Day_Tm  between  '08:00:00' and '08:59:59'  then 8
when Tm_Of_Day_Tm  between  '09:00:00' and '09:59:59'  then 9
when Tm_Of_Day_Tm  between  '10:00:00' and '10:59:59'  then 10
when Tm_Of_Day_Tm  between  '11:00:00' and '11:59:59'  then 11
when Tm_Of_Day_Tm  between  '12:00:00' and '12:59:59'  then 12
when Tm_Of_Day_Tm  between  '13:00:00' and '13:59:59'  then 13
when Tm_Of_Day_Tm  between  '14:00:00' and '14:59:59'  then 14
when Tm_Of_Day_Tm  between  '15:00:00' and '15:59:59'  then 15
when Tm_Of_Day_Tm  between  '16:00:00' and '16:59:59'  then 16
when Tm_Of_Day_Tm  between  '17:00:00' and '17:59:59'  then 17
when Tm_Of_Day_Tm  between  '18:00:00' and '18:59:59'  then 18
when Tm_Of_Day_Tm  between  '19:00:00' and '19:59:59'  then 19
when Tm_Of_Day_Tm  between  '20:00:00' and '20:59:59'  then 20
when Tm_Of_Day_Tm  between  '21:00:00' and '21:59:59'  then 21
when Tm_Of_Day_Tm  between  '22:00:00' and '22:59:59'  then 22
when Tm_Of_Day_Tm  between  '23:00:00' and '23:59:59'  then 23
end as Hours_of_Use,
count(*) as No_of_Data_Sessions,
sum(meas_Amt) as Total_DATA_MB
                    
    
From AUPR_MODEL_VIEW.Usg_Acty_Item uai
	
inner join Data_GSMSI_Key b
on uai.GSMSI_Key  =b.GSMSI_Key  

inner join AUPR_MODEL_VIEW.Non_Fin_Meas c
on uai.Acty_Item_Id=c.Acty_Item_Id
	
Where 1=1
--and uai.Usg_Type_Cd='AU_RETAIL_POSTPAID'
and cal_dt between '2019-08-01' and '2019-08-31'
and subs_id in (sel subs_id  from  All_Subs )
and Hours_of_Use in (11,12,1,2,3,4,5,6)
--and c.Meas_Type_key=4
group by 1,2,3,4,5,6,7,8,9,10,11,12
)with data 
primary index(subs_id)
on commit preserve rows;


Create volatile table Aug19_Postpay_All_Hourly_Usage as
(
Sel     
Acct_Id          ,             
Accs_Num    ,      
subs_id,       
Cal_Dt         ,        
Fin_Rcgntn_Dt      ,  
Usg_Dirctn_Key    ,   
Prim_Prod_Key   ,              
Prod_Key       ,                       
uai.GSMSI_Key     ,     
Usg_Class_Cd_c,
Usg_Class_Cd_f    ,             
case when Tm_Of_Day_Tm  between '00:00:00' and '00:59:59'  then 0
when Tm_Of_Day_Tm  between  '01:00:00' and '01:59:59' then 1
when Tm_Of_Day_Tm  between  '02:00:00' and '02:59:59'  then 2
when Tm_Of_Day_Tm  between  '03:00:00' and '03:59:59'  then 3
when Tm_Of_Day_Tm  between  '04:00:00' and '04:59:59'  then 4
when Tm_Of_Day_Tm  between  '05:00:00' and '05:59:59'  then 5
when Tm_Of_Day_Tm  between  '06:00:00' and '06:59:59'  then 6
when Tm_Of_Day_Tm  between  '07:00:00' and '07:59:59'  then 7
when Tm_Of_Day_Tm  between  '08:00:00' and '08:59:59'  then 8
when Tm_Of_Day_Tm  between  '09:00:00' and '09:59:59'  then 9
when Tm_Of_Day_Tm  between  '10:00:00' and '10:59:59'  then 10
when Tm_Of_Day_Tm  between  '11:00:00' and '11:59:59'  then 11
when Tm_Of_Day_Tm  between  '12:00:00' and '12:59:59'  then 12
when Tm_Of_Day_Tm  between  '13:00:00' and '13:59:59'  then 13
when Tm_Of_Day_Tm  between  '14:00:00' and '14:59:59'  then 14
when Tm_Of_Day_Tm  between  '15:00:00' and '15:59:59'  then 15
when Tm_Of_Day_Tm  between  '16:00:00' and '16:59:59'  then 16
when Tm_Of_Day_Tm  between  '17:00:00' and '17:59:59'  then 17
when Tm_Of_Day_Tm  between  '18:00:00' and '18:59:59'  then 18
when Tm_Of_Day_Tm  between  '19:00:00' and '19:59:59'  then 19
when Tm_Of_Day_Tm  between  '20:00:00' and '20:59:59'  then 20
when Tm_Of_Day_Tm  between  '21:00:00' and '21:59:59'  then 21
when Tm_Of_Day_Tm  between  '22:00:00' and '22:59:59'  then 22
when Tm_Of_Day_Tm  between  '23:00:00' and '23:59:59'  then 23
end as Hours_of_Use,
count(*) as No_of_Data_Sessions,
sum(meas_Amt) as Total_DATA_MB
                    
    
From AUPR_MODEL_VIEW.Usg_Acty_Item uai
	
inner join Data_GSMSI_Key b
on uai.GSMSI_Key  =b.GSMSI_Key  

inner join AUPR_MODEL_VIEW.Non_Fin_Meas c
on uai.Acty_Item_Id=c.Acty_Item_Id
	
Where 1=1
--and uai.Usg_Type_Cd='AU_RETAIL_POSTPAID'
and cal_dt between '2019-08-01' and '2019-08-10'
and subs_id in (sel subs_id  from  All_Subs )
--and Hours_of_Use in (11,12,1,2,3,4,5,6)
--and c.Meas_Type_key=4
group by 1,2,3,4,5,6,7,8,9,10,11,12
)with data 
primary index(subs_id)
on commit preserve rows;


INSERT INTO  Aug19_Postpay_All_Hourly_Usage
Sel     
Acct_Id          ,             
Accs_Num    ,      
subs_id,       
Cal_Dt         ,        
Fin_Rcgntn_Dt      ,  
Usg_Dirctn_Key    ,   
Prim_Prod_Key   ,              
Prod_Key       ,                       
uai.GSMSI_Key     ,     
Usg_Class_Cd_c,
Usg_Class_Cd_f    ,             
case when Tm_Of_Day_Tm  between '00:00:00' and '00:59:59'  then 0
when Tm_Of_Day_Tm  between  '01:00:00' and '01:59:59' then 1
when Tm_Of_Day_Tm  between  '02:00:00' and '02:59:59'  then 2
when Tm_Of_Day_Tm  between  '03:00:00' and '03:59:59'  then 3
when Tm_Of_Day_Tm  between  '04:00:00' and '04:59:59'  then 4
when Tm_Of_Day_Tm  between  '05:00:00' and '05:59:59'  then 5
when Tm_Of_Day_Tm  between  '06:00:00' and '06:59:59'  then 6
when Tm_Of_Day_Tm  between  '07:00:00' and '07:59:59'  then 7
when Tm_Of_Day_Tm  between  '08:00:00' and '08:59:59'  then 8
when Tm_Of_Day_Tm  between  '09:00:00' and '09:59:59'  then 9
when Tm_Of_Day_Tm  between  '10:00:00' and '10:59:59'  then 10
when Tm_Of_Day_Tm  between  '11:00:00' and '11:59:59'  then 11
when Tm_Of_Day_Tm  between  '12:00:00' and '12:59:59'  then 12
when Tm_Of_Day_Tm  between  '13:00:00' and '13:59:59'  then 13
when Tm_Of_Day_Tm  between  '14:00:00' and '14:59:59'  then 14
when Tm_Of_Day_Tm  between  '15:00:00' and '15:59:59'  then 15
when Tm_Of_Day_Tm  between  '16:00:00' and '16:59:59'  then 16
when Tm_Of_Day_Tm  between  '17:00:00' and '17:59:59'  then 17
when Tm_Of_Day_Tm  between  '18:00:00' and '18:59:59'  then 18
when Tm_Of_Day_Tm  between  '19:00:00' and '19:59:59'  then 19
when Tm_Of_Day_Tm  between  '20:00:00' and '20:59:59'  then 20
when Tm_Of_Day_Tm  between  '21:00:00' and '21:59:59'  then 21
when Tm_Of_Day_Tm  between  '22:00:00' and '22:59:59'  then 22
when Tm_Of_Day_Tm  between  '23:00:00' and '23:59:59'  then 23
end as Hours_of_Use,
count(*) as No_of_Data_Sessions,
sum(meas_Amt) as Total_DATA_MB
                    
    
From AUPR_MODEL_VIEW.Usg_Acty_Item uai
	
inner join Data_GSMSI_Key b
on uai.GSMSI_Key  =b.GSMSI_Key  

inner join AUPR_MODEL_VIEW.Non_Fin_Meas c
on uai.Acty_Item_Id=c.Acty_Item_Id
	
Where 1=1
--and uai.Usg_Type_Cd='AU_RETAIL_POSTPAID'
and cal_dt between '2019-08-11' and '2019-08-20'
and subs_id in (sel subs_id  from  All_Subs )
--and Hours_of_Use in (11,12,1,2,3,4,5,6)
--and c.Meas_Type_key=4
group by 1,2,3,4,5,6,7,8,9,10,11,12;

INSERT INTO  Aug19_Postpay_All_Hourly_Usage
Sel     
Acct_Id          ,             
Accs_Num    ,      
subs_id,       
Cal_Dt         ,        
Fin_Rcgntn_Dt      ,  
Usg_Dirctn_Key    ,   
Prim_Prod_Key   ,              
Prod_Key       ,                       
uai.GSMSI_Key     ,     
Usg_Class_Cd_c,
Usg_Class_Cd_f    ,             
case when Tm_Of_Day_Tm  between '00:00:00' and '00:59:59'  then 0
when Tm_Of_Day_Tm  between  '01:00:00' and '01:59:59' then 1
when Tm_Of_Day_Tm  between  '02:00:00' and '02:59:59'  then 2
when Tm_Of_Day_Tm  between  '03:00:00' and '03:59:59'  then 3
when Tm_Of_Day_Tm  between  '04:00:00' and '04:59:59'  then 4
when Tm_Of_Day_Tm  between  '05:00:00' and '05:59:59'  then 5
when Tm_Of_Day_Tm  between  '06:00:00' and '06:59:59'  then 6
when Tm_Of_Day_Tm  between  '07:00:00' and '07:59:59'  then 7
when Tm_Of_Day_Tm  between  '08:00:00' and '08:59:59'  then 8
when Tm_Of_Day_Tm  between  '09:00:00' and '09:59:59'  then 9
when Tm_Of_Day_Tm  between  '10:00:00' and '10:59:59'  then 10
when Tm_Of_Day_Tm  between  '11:00:00' and '11:59:59'  then 11
when Tm_Of_Day_Tm  between  '12:00:00' and '12:59:59'  then 12
when Tm_Of_Day_Tm  between  '13:00:00' and '13:59:59'  then 13
when Tm_Of_Day_Tm  between  '14:00:00' and '14:59:59'  then 14
when Tm_Of_Day_Tm  between  '15:00:00' and '15:59:59'  then 15
when Tm_Of_Day_Tm  between  '16:00:00' and '16:59:59'  then 16
when Tm_Of_Day_Tm  between  '17:00:00' and '17:59:59'  then 17
when Tm_Of_Day_Tm  between  '18:00:00' and '18:59:59'  then 18
when Tm_Of_Day_Tm  between  '19:00:00' and '19:59:59'  then 19
when Tm_Of_Day_Tm  between  '20:00:00' and '20:59:59'  then 20
when Tm_Of_Day_Tm  between  '21:00:00' and '21:59:59'  then 21
when Tm_Of_Day_Tm  between  '22:00:00' and '22:59:59'  then 22
when Tm_Of_Day_Tm  between  '23:00:00' and '23:59:59'  then 23
end as Hours_of_Use,
count(*) as No_of_Data_Sessions,
sum(meas_Amt) as Total_DATA_MB
                    
    
From AUPR_MODEL_VIEW.Usg_Acty_Item uai
	
inner join Data_GSMSI_Key b
on uai.GSMSI_Key  =b.GSMSI_Key  

inner join AUPR_MODEL_VIEW.Non_Fin_Meas c
on uai.Acty_Item_Id=c.Acty_Item_Id
	
Where 1=1
--and uai.Usg_Type_Cd='AU_RETAIL_POSTPAID'
and cal_dt between '2019-08-21' and '2019-08-31'
and subs_id in (sel subs_id  from  All_Subs )
--and Hours_of_Use in (11,12,1,2,3,4,5,6)
--and c.Meas_Type_key=4
group by 1,2,3,4,5,6,7,8,9,10,11,12;


drop table BI_ANALYTICS_TEMP_DB.SG_NightUsage_WeekGroup;
Create  table BI_ANALYTICS_TEMP_DB.SG_NightUsage_WeekGroup as
(
sel 
subs_id,
case when Clndr_Day_Of_Week in (5,6) then 'Fri-Sat' else 'Sun-Thu' end as Week_Group,
/*Clndr_Week_Of_Year,
Clndr_Day_Desc,*/
sum(Total_DATA_MB) as Sum_Total_DATA_MB,
sum(No_of_Data_Sessions) as Sum_No_of_Data_Sessions
from 
(sel a.*,
Clndr_Day_Of_Week,
Clndr_Day_Desc,
Clndr_Week_Of_Year

 from   Aug19_Postpay_Hourly_Usage a

inner join   aupr_model_view.edm_clndr_dt b
on a.Cal_dt=b.Clndr_dt)a
group by 1,2
)with data 
primary index(subs_id);

sel * from NightUsage_WeekGroup;

drop table BI_ANALYTICS_TEMP_DB.SG_AllUsage_WeekGroup;
Create  table BI_ANALYTICS_TEMP_DB.SG_AllUsage_WeekGroup as
(
sel 
subs_id,
case when Clndr_Day_Of_Week in (5,6) then 'Fri-Sat' else 'Sun-Thu' end as Week_Group,
/*Clndr_Week_Of_Year,
Clndr_Day_Desc,*/
sum(Total_DATA_MB) as Sum_Total_DATA_MB,
sum(No_of_Data_Sessions) as Sum_No_of_Data_Sessions
from 
(sel a.*,
Clndr_Day_Of_Week,
Clndr_Day_Desc,
Clndr_Week_Of_Year

 from   Aug19_Postpay_All_Hourly_Usage a

inner join   aupr_model_view.edm_clndr_dt b
on a.Cal_dt=b.Clndr_dt)a
group by 1,2
)with data 
primary index(subs_id);


Create volatile table NightUsage_Percentage as
(
sel a.*,
b.Sum_Total_DATA_MB as All_Data_MB,
b.Sum_No_of_Data_Sessions as All_Data_Sessions,
case when b.Sum_Total_DATA_MB>0 then  a.Sum_Total_DATA_MB*100/b.Sum_Total_DATA_MB else 0 end as Per_Total_Data_MB

from BI_ANALYTICS_TEMP_DB.SG_NightUsage_WeekGroup a

left join BI_ANALYTICS_TEMP_DB.SG_AllUsage_WeekGroup b
on a.subs_id=b.subs_id
and a.Week_Group=b.Week_Group
)with data 
primary index(subs_id,Week_Group)
on commit preserve rows;


drop table BI_ANALYTICS_TEMP_DB.SG_Nightowls;
Create  table BI_ANALYTICS_TEMP_DB.SG_Nightowls as
(
sel a.*,
b.Sum_Total_Data_MB as Sun_Thu_11_6_Usage,
case when b.Sum_Total_Data_MB>0 and  a.Sum_Total_Data_MB/b.Sum_Total_Data_MB>0.30 and a.Per_Total_Data_MB>0.30 then 1 
when b.Sum_Total_Data_MB=0 and a.Sum_Total_Data_MB>0 then 1
else 0 end as Night_Owl

 from   (sel * from NightUsage_Percentage where Week_Group='Fri-Sat') a

left  join  (sel * from NightUsage_Percentage where Week_Group='Sun-Thu')  b
on a.subs_id=b.subs_id
)with data 
primary index(subs_id);


	
	/*** Changing In Use Device**/
--drop table All_Subs;
Create volatile table All_Subs as
(
sel subs_id,
ACCT_TYPE_CD  ,                
PRIM_PROD_KEY,                 
DLR_CD  ,                                        
GOLDEN_PHYS_ADDR_ID           

FROM aupr_bus_view.Fact_Ppltn_Snap c
    where 
    (prd_dt = '2019-08-24')
	and prd_type_cd='DAY' 
    and ppltn_type_id=1
    and brnd_cd='Vodafone' 
    ---and Acct_type_Cd = 'Postpay'
    and subs_id>0
)with data 
primary index(subs_id)
on commit preserve rows;
	
   --drop table Device_Data;
	Create volatile table Device_Data as
	(
	sel 
	a.subs_id
	,b.ACCT_TYPE_CD                 
	,b.PRIM_PROD_KEY                 
	,b.DLR_CD                                          
	,b.GOLDEN_PHYS_ADDR_ID           
	,In_Use_Dvc_Prod_Cd     
	,c.Mnfctr_Nm as In_Use_Mnfctr_Nm
	,c.Dvc_Long_Nm as In_Use_Dvc_Long_Nm
	,c.Grp_Smart_Ph_Ind as In_Use_Grp_Smart_Ph_Ind
	,c.OS_Nm as In_Use_OS_Nm
	,c.OS_Ver_Nm as In_Use_OS_Ver_Nm
	,In_Use_Type_Aprvl_Cd          
	,Curr_Dvc_IMEI_Id              
	,Prev_Dvc_IMEI_Id              
	,Prev_In_Use_Dvc_Prod_Cd  
	,Prvsnd_Dvc_Prod_Cd            
	,d.Mnfctr_Nm as Prvsnd_Dvc_Mnfctr_Nm
	,d.Dvc_Long_Nm as Prvsnd_Dvc_Dvc_Long_Nm
	,d.Grp_Smart_Ph_Ind as Prvsnd_Dvc_Grp_Smart_Ph_Ind
	,d.OS_Nm as Prvsnd_Dvc_OS_Nm
	,d.OS_Ver_Nm as Prvsnd_Dvc_OS_Ver_Nm
	,Prvsnd_Dvc_Prod_Key           
	,Prev_Prvsnd_Dvc_Prod_Key      
	,Prev_Prvsnd_Dvc_Prod_Cd       
	,Prev_Type_Aprvl_Cd            
	,In_Use_Dvc_Start_Dt   
	,Dim_start_dt
	,Dim_end_dt
	
	from  AUPR_BUS_VIEW.DIM_SUBS a
	
	inner join All_Subs b
	on a.subs_id=b.subs_id
	and '2019-08-24' between Dim_Start_dt and DIm_end_dt
	
	left join AUPR_BUS_VIEW.Dim_Dvc_Prod c
	on a.In_Use_Dvc_Prod_Cd=c.Dvc_Prod_Cd
	
    left join AUPR_BUS_VIEW.Dim_Dvc_Prod d
	on a.Prvsnd_Dvc_Prod_Cd=d.Dvc_Prod_Cd
	
	)with data 
	primary index(subs_id)
	on commit preserve rows;
	
	
    drop table BI_ANALYTICS_TEMP_DB.SG_Device_Data_History;
	Create table BI_ANALYTICS_TEMP_DB.SG_Device_Data_History as
	(
	sel 
	a.*
	,b.In_Use_Dvc_Prod_Cd    as  In_Use_Dvc_Prod_Cd_1
	,c.Mnfctr_Nm as In_Use_Mnfctr_Nm_1
	,c.Dvc_Long_Nm as In_Use_Dvc_Long_Nm_1
	,c.Grp_Smart_Ph_Ind as In_Use_Grp_Smart_Ph_Ind_1
	,c.OS_Nm as In_Use_OS_Nm_1
	,c.OS_Ver_Nm as In_Use_OS_Ver_Nm_1           
	,b.In_Use_Dvc_Start_Dt   as In_Use_Dvc_Start_dt_1
	,b.Dim_start_dt as Dim_start_dt_1
	,b.Dim_end_dt as Dim_end_dt_1
	
	from  Device_Data a
	
	left join  AUPR_BUS_VIEW.DIM_SUBS b
	on a.subs_id=b.subs_id
	and a.In_Use_Dvc_Prod_Cd<>b.In_Use_Dvc_Prod_Cd
	and a.In_Use_Dvc_Start_Dt>b.In_Use_Dvc_Start_Dt

	left join AUPR_BUS_VIEW.Dim_Dvc_Prod c
	on b.In_Use_Dvc_Prod_Cd=c.Dvc_Prod_Cd
	
	qualify(rank() over (partition by b.subs_id order by  b.Dim_start_dt desc))=1
	
	)with data 
	primary index(subs_id);


sel *  from BI_ANALYTICS_TEMP_DB.SG_Top3Cluster_Commuter_Data
sel *  from BI_ANALYTICS_TEMP_DB.SG_Nightowls
sel *   from TEMP_DB.SG_Packet_data_Flag
sel * from  BI_ANALYTICS_TEMP_DB.SG_Device_Data_History



	/*** Backpackers **/
drop table All_Subs_Dealer;
Create volatile table All_Subs_Dealer as
(
sel subs_id,
c.Acct_type_Cd,
c.PRIM_PROD_KEY,                                                         
c.GOLDEN_PHYS_ADDR_ID ,          
c.dlr_cd,
Dlr_Nm,
Dlr_Addr_Ln_1_Nm,
Dlr_Addr_Ln_2_Nm,
Dlr_Addr_Ln_3_Nm,
Dlr_Sbrb_Nm,
Dlr_State_Nm,
Dlr_Pstcd_Cd,
Dlr_Type_Cd,
Dlr_Type_Nm,
prd_dt
FROM aupr_bus_view.Fact_Ppltn_Snap c 

inner join  aupr_bus_view.Dim_Dlr d
on c.Dlr_Cd=d.Dlr_Cd
    where 
    (prd_dt = '2019-08-24')
	and prd_type_cd='DAY' 
    and ppltn_type_id=1
    and brnd_cd='Vodafone' 
    and subs_id>0
)with data 
primary index(subs_id)
on commit preserve rows;

sel * from All_Subs_Dealer where Dlr_Nm like '%airport%'

sel * from dbc.columns where tablename like '%play%'

drop table   BI_ANALYTICS_TEMP_DB.SG_Airport_Connect_SA2;
Create  table   BI_ANALYTICS_TEMP_DB.SG_Airport_Connect_SA2 as 
(
sel a.*
,b.Acct_type_Cd
,b.PRIM_PROD_KEY                                                      
,b.GOLDEN_PHYS_ADDR_ID        
,b.Dlr_Nm
,b.Dlr_Addr_Ln_1_Nm
,b.Dlr_Addr_Ln_2_Nm
,b.Dlr_Addr_Ln_3_Nm
,b.Dlr_Sbrb_Nm
,b.Dlr_State_Nm
,b.Dlr_Pstcd_Cd
,b.Dlr_Type_Cd
,b.Dlr_Type_Nm
,pi.Idfcn_Ref_Val_1
,pi.Idfcn_Ref_Val_2
,pi.Idfcn_Expiry_Dt
,it.Idfcn_Type_desc

 from All_Subs_Dealer  b
 
inner join AUPR_BUS_VIEW.Dim_subs_sa2 a
 on a.subs_id=b.subs_id
 and b.prd_dt between a.dim_start_dt and a.dim_end_dt
 
inner join   aupr_bus_view.DIM_SUBS ds
    on b.subs_id=ds.subs_id
    and b.prd_dt between  ds.dim_start_dt and ds.dim_end_dt
 
 left join aupr_model_view.Party_Idfcn pi
on ds.Acct_Party_Id  =pi.Party_Id 

left join AUPR_MODEL_VIEW.Idfcn_Type it
on pi.Idfcn_Type_Id=it.Idfcn_Type_id
 
 qualify(rank() over (partition by b.subs_id order by a.dim_start_dt desc,a.dim_end_dt desc,Idfcn_Expiry_Dt desc,Idfcn_Type_desc desc))=1
 
)with data 
primary index(subs_id);


/**NPS Prediction Data**/
Create table BI_ANALYTICS_TEMP_DB.SG_NPS_Prediction as
(
sel * from BI_ANALYTICS_TEMP_DB.SG_NPS
union
sel * from BI_ANALYTICS_TEMP_DB.SG_NPS2
union
sel * from BI_ANALYTICS_TEMP_DB.SG_NPS3
)with data 
primary index(subs_id);

 
 
 Create volatile table ShoppingCenter_IKEA_Flag as
 (
 sel distinct subs_id
,In_ShoppingCenter_Flag  
,In_IKea_Flag
from TEMP_DB.SG_Packet_data_Flag
  )with data 
 primary index(subs_id)
 on commit preserve rows;
 
 sel * from TEMP_DB.SG_Packet_data_Flag
 
 sel * from   BI_ANALYTICS_TEMP_DB.SG_Nightowls where subs_id in (sel id_subs_id from   Churn_data )
 
 
 drop table bi_analytics_temp_db.Subscriber_Features_Prod;
 Create table bi_analytics_temp_db.Subscriber_Features_Prod as
 (
 sel a.*
,(a.prd_dt-In_Use_Dvc_Start_Dt)/30 as Current_Inuse_Dvc_Tenure
,(In_Use_Dvc_Start_Dt-In_Use_Dvc_Start_Dt_1)/30 as Previous_Inuse_Dvc_Tenure
,Cluster_Name    as Top_Cluster_name               
,sum_Dwnld_Vol_Kbs  as Top_Data_Usage_MB            
,Top2_Cluster_Name             
,Top2_sum_Dwnld_Vol_Kbs  as  Top2_Data_Usage_MB        
,Top3_Cluster_Name             
,Top3_sum_Dwnld_Vol_Kbs   as   Top3_Data_Usage_MB          
,case when Commuter_Cluster_Name is not null then 1 else 0 end as Commuter_Flag        
,Night_Owl                     
,In_ShoppingCenter_Flag  
,In_IKea_Flag
 
 from Churn_data  a
 
 left join BI_ANALYTICS_TEMP_DB.SG_Device_Data_History  b
 on a.ID_subs_id=b.subs_id
 
 left join   BI_ANALYTICS_TEMP_DB.SG_Top3Cluster_Commuter_Data c
 on a.ID_subs_id=c.subs_id
 
left join  BI_ANALYTICS_TEMP_DB.SG_Nightowls d
on a.ID_subs_id=d.subs_id

left join    ShoppingCenter_IKEA_Flag e
on a.ID_subs_id=e.subs_id

qualify (rank() over (partition by a.ID_subs_id order by
Current_Inuse_Dvc_Tenure
,Previous_Inuse_Dvc_Tenure
,Top_Cluster_name               
,Top_Data_Usage_MB            
,Top2_Cluster_Name             
,Top2_Data_Usage_MB        
,Top3_Cluster_Name             
,Top3_Data_Usage_MB          
,Commuter_Flag desc       
,Night_Owl     desc                
,In_ShoppingCenter_Flag desc 
,In_IKea_Flag desc))=1
  )with data 
 primary index(ID_subs_id);

/** 3G Percentage */
CREATE VOLATILE TABLE pct_G_total AS
(
select * from
(
    select
    subs_id,
    --EXTRACT(MONTH FROM Cal_Dt) montho,
    100.00000*sum(CASE 
     Last_Radio_Type_Cd WHEN 'U' 
    THEN (Upld_Vol_Kbytes+Dwnld_Vol_Kbytes) 
     ELSE 0
     END) /
    sum(Upld_Vol_Kbytes+Dwnld_Vol_Kbytes) pct_3G_total ,
     
    100.00000*sum(CASE 
     Last_Radio_Type_Cd WHEN 'L' 
    THEN (Upld_Vol_Kbytes+Dwnld_Vol_Kbytes) 
     ELSE 0
     END) /
    sum(Upld_Vol_Kbytes+Dwnld_Vol_Kbytes) pct_4G_total 
     
    FROM aupr_model_view.PCKT_NTWK_USG_ACTY_ITEM A
    where 
    Usg_Src_Cd='PGW-CDR'
    and MCC = '505'
    and IMEI_ID is not NULL
    and Roamg_Type_Cd ='DOM'
    -- and montho = EXTRACT(MONTH FROM Cal_Dt)
    -- EXTRA BITS TO SAVE TIME
    AND EXTRACT(YEAR FROM Cal_Dt)  = EXTRACT(YEAR FROM ADD_MONTHS('2019-09-24',-2)) AND EXTRACT(MONTH FROM Cal_Dt)  IN EXTRACT(MONTH FROM ADD_MONTHS('2019-09-24',-2))
    --AND EXISTS(SELECT 1 FROM base_fps B WHERE A.subs_id = B.Subs_id)
    group by-- montho, 
        subs_id
    ) pct_G
) WITH DATA ON COMMIT PRESERVE ROWS;

sel * from  pct_G_total


 drop table bi_analytics_temp_db.Subscriber_Features_Dev;
 Create table  bi_analytics_temp_db.Subscriber_Features_Dev as
 (
 sel a.*,
 b.predicted_nps_cat,
 c.pct_3G_total,
 c.pct_4G_total
 
 from   bi_analytics_temp_db.Subscriber_Features_Prod  a
 
 left join BI_ANALYTICS_TEMP_DB.SG_NPS_Prediction b
 on a.Id_subs_id=b.subs_id
 
 left join pct_G_total c
 on a.Id_subs_id=c.subs_id
 
 )with data 
  primary index(ID_subs_id);
  
  sel * from bi_analytics_temp_db.Subscriber_Features_Dev;
 
 