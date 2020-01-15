use cms;
drop table if exists cms.abrechnungen;
create table cms.abrechnungen (arzt DOUBLE PRECISION, fachgebiet DOUBLE PRECISION, 
                                krankenhaus_oder_arzt DOUBLE PRECISION, medikament DOUBLE PRECISION, strittig INT,
                                bundesland DOUBLE PRECISION, leistungsempfaenger_plz DOUBLE PRECISION, 
                                auszahlung_monat DOUBLE PRECISION, auszahlung_euro INT) 
STORED AS PARQUET 
LOCATION '/tmp/cmsml/'
TBLPROPERTIES ('external.table.purge'='true')
;
INSERT OVERWRITE TABLE cms.abrechnungen select 
cast( abs( hash(physician_profile_id))/9999999999999999999 as DOUBLE PRECISION)                arzt,
cast( abs( hash(physician_specialty))/9999999999999999999 as DOUBLE PRECISION)                  fachgebiet,
cast( abs( hash(covered_recipient_type))/9999999999999999999 as DOUBLE PRECISION)               krankenhaus_oder_arzt, 
cast( abs( hash(associated_drug_or_biological_ndc_1))/9999999999999999999 as DOUBLE PRECISION)  medikament,
 case  when dispute_status_for_publication ='"No"' then 0 else 1 end                             strittig,
cast( abs( hash(  recipient_state ))/9999999999999999999 as DOUBLE PRECISION)                    bundesland,
cast( abs( hash(  substr( recipient_zip_code, 2, 5 )))/9999999999999999999 as DOUBLE PRECISION) leistungsempfaenger_plz,
cast( abs( hash( substr(date_of_payment,3)))/9999999999999999999 as DOUBLE PRECISION)           auszahlung_monat,
cast (total_amount_of_payment_usdollars as int)                                              auszahlung_euro
from cms.generalpayments
;
select * from cms.abrechnungen limit 3
;
