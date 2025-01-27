-- ///////////////////////////////////////////////////////////////
--
-- Database schema for TDM23.
--
-- ///////////////////////////////////////////////////////////////

-- Clean up anything that may have been left lying around, so we start with a clean slate:
--
drop table if exists MA_taz_geography;
drop table if exists taz_2010block_allocation;
drop table if exists taz_2010block_assignment;
drop table if exists tazpuma;
drop table if exists walkbike;
drop table if exists parking;
drop table if exists enrollment;
drop table if exists special_generators;
drop table if exists access_density;
drop table if exists terminal_times;
drop table if exists emp_access;
drop table if exists hh_per;	-- This table will no longer be created. This statement is to remove a vestigial hh_per table, if one exists.
drop table if exists block_sed;
drop table if exists hh;
drop table if exists per;
drop table if exists veh;
drop table if exists wfh;
drop table if exists jobs;
drop table if exists trip_prod;
drop table if exists prod_nhb;
drop table if exists trip_attr;
drop table if exists trip_prod_pknp;
drop table if exists trip_prod_nhb_pknp;
drop table if exists sg_prod;
drop table if exists sg_attr;

--
-- Tables defined in the TAZ tab of the TDM23 HLD Google Sheet.
--
-- TAZ table (attributes only, no geography)
create table MA_taz_geography (
	taz_id			integer primary key,	-- TAZ ID
	type		text,					-- Internal ('I') or External ('E') zone
	town		text,					-- Name of municipality, capitalzied as in MassGIS TOWNS_POLYM layer
	state    	text,					-- Two-letter USPS state abbreviation
	town_state 	text,					-- For reporting: municipality + ', ' + state
	mpo			text,					-- Abbreviation of MPO name, empty-text for towns outside of MA
	in_brmpo	integer,				-- Dummy variable: 1 if mpo == 'BRMPO', otherwise 0
	subregion	text,					-- MAPC subregion, or empty string for towns outside MAPC region
	corridor	integer,				-- Radial corridor ID, empty-string for towns outside of old CTPS 164-town "model area"
	-- corr_name	text,				-- For reporting: name of radial corridor
	ring		integer,				-- Circumferential ring ID
	district	integer,				-- District (group of TAZes) ID
	total_area	float,					-- Total area, in square miles
	land_area	float, 				    -- Land area, in square miles
	urban       integer );              -- overlap with UZA (1), else rural (0)
	
-- taz_block_allocation table: Allocation of 2010 Census Blocks to TAZs
--
create table taz_block_allocation (
	taz_id		integer,	-- TAZ ID
	block_id	text,		-- Block ID
	area_fct	float );	-- The fraction of the land area (total area in the case of water-only blocks) of the block overlapped in TAZ
	
-- taz_block_allocation table: Assignment of a single TAZ to each 2010 Census block
--
create table taz_block_assignment (
	block_id	text,		-- Block ID
	taz_id		integer );	-- TAZ ID
	
-- tazpuma table: assignment of a single 2010 PUMA to each TAZ
--
create table tazpuma(
	taz_id      integer, 	-- TAZ ID
	puma00		text,		-- 2000 PUMA ID
	puma10		text );		-- 2010 PUMA ID

-- Input data 
--
-- walk-ability/bike-ability table
--
create table walkbike (
	taz_id		integer primary key,	-- TAZ ID
	walkability	float,					-- Measure of walk environment
	bikeability	float );				-- Measure of bike environment
	
-- parking table  
-- 
create table parking (
	taz_id		integer primary key,	-- TAZ ID
	capacity	integer,				-- Number of off-street spaces
	cost_hr		float,					-- Parking cost (hourly)
	cost_dr		float,					-- Parking cost (daily)
	cost_mr		float);					-- Parking cost (monthly)

-- school enrollment table  
-- 
create table enrollment (
	taz_id			integer primary key,	-- TAZ ID
	k12				integer, 				-- K-12 enrollment
	college_total	integer,				-- Total college students
	college_commuter integer				-- Off-campus college students
	);
	

-- Modeled data 
-- 
-- access_density table 
-- 
create table access_density (
	taz_id			integer primary key,	-- TAZ ID
	access_density	integer );				-- 1-5 measure of density and access to transit
	
-- terminal_times table 
--
create table terminal_times (
	taz_id				integer primary key,	-- TAZ ID
	terminal_time_p 	float,					-- OVTT for auto access
	terminal_time_a		float, 					-- OVTT for auto egress
	rs_wait_time		float );				-- OVTT for ride source access
	
-- employee access table 
--
create table emp_access (
	taz_id		integer primary key,	-- TAZ ID
	pctemp10a	float,					-- percent of employment within 10 minutes by auto
	pctemp30a	float,					-- percent of employment within 30 minutes by auto
	pctemp30t	float );				-- percent of employment within 30 minutes by transit
	
-- Tables defined in the "Socioeconomic" tab of the TDM23 HLD Google Sheet.
--
-- hh_per - UrbanSim output containing both "person" and "household" records
-- Note: This table is NOT created.
--       The following commented-out 'create table' statement only documents the data types
--       of the columns in the input CSV file used to generate the 'hh' and 'per' tables. (See below.)
--create table hh_per (
--	serialno					integer primary key,	-- Housing unit/GQ person serial number
--	tenure						integer,				-- N/A (GQ/vacant), 
--														-- (1) Owned with mortgage or loan (include home equity loans)
--														-- (2) Owned free and clear, (3) Rented, (4) Occupied without payment of rent
--	persons						integer,				-- Number of persons in family (unweighted)
--	block_id					text,					-- 2010 Census Block FIPS. Must correspond to the block_ids in the block attribute table.
--	race_of_head				integer,				-- Race code of head of household (See PUMS RAC1P variable lookup and 4)
--	age_of_head					integer,				-- Age of head of household (See 4)
--	income						integer,				-- Annual household income in 2013 dollars (See HINCP NP variable)
--	children					integer,				-- Number of persons under age 18 in household (See PUMS AGEP variable 6)
--	workers						integer,				-- Number of workers (employed persons) in the household (See PUMS ESR variable 5)
--	recent_mover				integer,				-- Boolean: 1 if household moved within last 5 years, else 0 (See PUMS MV variable 7)
--	hispanic_status_of_head		integer,				-- Boolean: 1 if head of household is hispanic, else 0 (See PUMS HISP variable 11)
--	row							integer,			
--	hid							text,					-- Household ID, E.g., 2009000000393_1
--	hh_adj						integer,				
--	inc_adj						integer,				
--	hh_type						integer,				
--	bld_type					integer,				
--	vacancy						integer,				
--	prop_value					integer,				
--	hh_inc						integer,				
--	workers_in_fam				integer,				
--	person_num					integer,				
--	age							integer,			
--	wage_inc					integer,					
--	other_inc					integer,					
--	emp_status					integer,					
--	naics						text,				
--	tot_inc_person				integer,					
--	gq							integer,			
--	age_grp						integer,				
--	is_worker					integer,					
--	blockgroup_id				text,						
--	year						integer);

-- block_sed table: block-level socioeconomic data table 
--
create table block_sed (
	block_id		text primary key,	-- Block ID (FIPS code)
	taz_id			integer,	-- TAZ ID
	"1_constr"		integer,	-- Total jobs in aggregate sector 1
	"10_ttu"			integer,	-- Total jobs in aggregate sector 10
	"2_eduhlth"		integer,	-- Total jobs in aggregate sector 2
	"3_finance"		integer,	-- Total jobs in aggregate sector 3
	"4_public"		integer,	-- Total jobs in aggregate sector 4
	"5_info"			integer,	-- Total jobs in aggregate sector 5
	"6_ret_leis"		integer,	-- Total jobs in aggregate sector 6
	"7_manu"			integer,	-- Total jobs in aggregate sector 7
	"8_other"			integer,	-- Total jobs in aggregate sector 8
	"9_profbus"		integer,	-- Total jobs in aggregate sector 9
	total_jobs					integer,	-- Total jobs
	total_households			integer	-- Total HH
    );

	
-- The "hh" ("household") table defined in the "Socioeconomic" tab of the TDM23 Google Sheet
-- 
create table hh (
	block_id			text,				-- block ID 
	taz_id				integer,			-- TAZ ID 
	hid					text primary key,	-- household ID
	persons				integer,			-- number of persons in household
	hh_inc				integer,			-- household income 
	hh_inc_cat_by_size	integer,			-- household size-based income category: 1 = low-income, 2 = medium-income, 3 = high-income
	children			integer,			-- number of children in household 
	seniors				integer,			-- number of persons age >= 65 in household
	nwseniors			integer,			-- number of non-working persons age >= 65 in household
	workers				integer,			-- number of workers in household 
	drivers				integer,			-- number of drivers in household
	nwadult				integer );			-- number of non working adults in household

	
	
-- The "per" ("person") table defined in the "Socioeconomic" tab of the TDM23 Google Sheet
-- 
create table per (					-- NOTE: This table currently has no primary key(!)
	block_id		text,			-- block ID
	taz_id			integer,		-- TAZ ID 
	hid				string,			-- household ID 
	person_num		integer,		-- person number (within household)
	age				integer,		-- age of person 
	wage_inc		integer,		-- ??
	is_worker		integer,		-- is person a worker?
	persons			integer,		-- number of persons in family (unweighted) 
	children		integer,		-- number of children
	workers			integer );		-- number of workers 
	
-- ///////////////////////////////////////////////////////////////
--
-- Tables defined in the WFH tab of the TDM23 HLD Google Sheet.
--
-- wfh table: worker commute equivalents
--
create table wfh (
	hid			text,		-- Household ID
	block_id	text,					-- Block ID	
	taz_id		integer, 			-- Zone ID for each block	
	person_num	integer,				-- Person ID (within HH ID)
	commute_eqs	float,					-- Commute equivalents - share of workdays person commutes
	wfh_eqs		float,
	PRIMARY KEY ( hid, person_num) );				-- Work from home equivalents - share of workdays person works from home
	
-- jobs table: job equivalents
--
create table jobs (
	block_id		text primary key,	-- Block ID (FIPS code)
	taz_id			integer,	-- TAZ ID
	"1_constr"		integer,	-- Total jobs in aggregate sector 1
	"10_ttu"			integer,	-- Total jobs in aggregate sector 10
	"2_eduhlth"		integer,	-- Total jobs in aggregate sector 2
	"3_finance"		integer,	-- Total jobs in aggregate sector 3
	"4_public"		integer,	-- Total jobs in aggregate sector 4
	"5_info"			integer,	-- Total jobs in aggregate sector 5
	"6_ret_leis"		integer,	-- Total jobs in aggregate sector 6
	"7_manu"			integer,	-- Total jobs in aggregate sector 7
	"8_other"			integer,	-- Total jobs in aggregate sector 8
	"9_profbus"		integer,	-- Total jobs in aggregate sector 9
	total_jobs		integer		-- total jobs (in person equivalents)
);



-- ///////////////////////////////////////////////////////////////
--
-- Tables defined in the "TG_db" tab of the TDM23 HLD Google Sheet.
--
-- Productions
--
-- trip_prod table - Trips produced by workers / household (if hh, person_num = null)
--
create table trip_prod (
	-- hid_pn		text primary key, -- Household ID + PersonID
	hid				text,			-- Household ID
	block_id 	text,					-- Block ID
	person_num 	integer,		-- Person ID (within HH ID)
	hbw_p 		float,				-- home based work trips
	hbsc_p		float,					-- home based school trips
	hbsr_p 		float,				-- home base social-recreation trips
	hbpb_p 		float,				-- home based personal business trips
	nhbw_p 		float,				-- non-home-based work-related trips
	nhbnw_p 	float,	 			-- non-home-based non-work trips
	PRIMARY KEY ( hid, person_num)
);
-- trip_prod_nhb - Non-home based trips produced by block (hh and workers trips)
--
create table prod_nhb (
	block_id	text primary key,	-- Block ID
	taz_id		integer, 			-- Zone ID for each block
	nhbw_p  	float,				-- non-home-based work-related trips
	nhbnw_p 	float );			-- non-home-based non-work trips

--
-- Attractions
--
-- trip_attr table - Trips attracted by segment and block
--
create table trip_attr (
	block_id	text primary key,		-- Block Id
	taz_id		integer,				-- zone id for each block
	hbw_inc1_a	float,					-- home basd work trips for worker income 1
	hbw_inc2_a	float,					-- home basd work trips for worker income 2
	hbw_inc3_a	float,					-- home basd work trips for worker income 3
	hbw_inc4_a	float,					-- home basd work trips for worker income 4
	hbsr_a		float,					-- home base social-recreation trips
	hbpb_a		float,					-- home based personal business trips
	hbsc_a		float,					-- home based school trips
	nhbw_a		float,					-- non-home-based work-related trips
	nhbnw_a		float );				-- non-home-based non-work trips


-- ///////////////////////////////////////////////////////////////
--
-- Tables defined in the "PkNp_db" tab of the TDM23 HLD Google Sheet.
--
-- Productions
-- 
-- trip_prod_pknp table - Trips produced by workers / household (if hh, person_num = null) 
--                        for peak and non-peak periods
--
create table trip_prod_pknp (
	hid 		text,		-- Household ID
	block_id 	text,					-- Block ID
	person_num 	integer,				-- Person ID (within HH ID)
	peak		integer,				-- Peak time period flag
	hbw_p		float,					-- home based work trips
	hbsr_p 		float,					-- home base social-recreation trips
	hbsc_p 		float,					-- home base school trips
	hbpb_p 		float,					-- home based personal business trips
	nhbw_p 		float,					-- non-home-based work-related trips
	nhbnw_p 	float,				-- non-home-based non-work trips
	PRIMARY KEY ( hid,person_num,peak)
	 );
-- trip_prod_nhb_pknp table - Non-home based trips produced by block for peak and non-peak periods
--
create table trip_prod_nhb_pknp (
	block_id	text ,					-- Block ID
	taz_id		integer,				-- Zone ID for each block
	peak		integer,				-- Peak period time FLAGGER
	nhbw_p		float,					-- non-home-based work-related trips
	nhbnw_p		float, 				-- non-home-based non-work trips
	PRIMARY KEY ( block_id, peak)
	 );			
	